import Cryptos
import NIOConcurrencyHelpers
import NIO
import NIOAdvanced
import NIOPosix
import Foundation
import AsyncHTTPClient
import NIOHTTP1

open class ReqClient<IOHandler>: @unchecked Sendable where IOHandler: RequestCryptoIOHandler {
    public let eventLoop: EventLoop
    public let fileEventLoop: EventLoop
    public let logger: Logger?
    public let byteBufferAllocator: ByteBufferAllocator
    public var ioHandler: IOHandler!
    public let storage: SendableStorage = .init()
    public weak var channel: Channel? {
        if let channel = __channel, channel.isActive { return channel }
        return nil
    }
    
    @usableFromInline
    private(set) weak var __channel: Channel?
    @usableFromInline
    private(set) var lock: NIOLock = .init()
    @usableFromInline
    let removableHandlerNames: [String] = [
        "Whooshing Crypto Handler",
        "NIO HTTPRequestEncoder",
        "NIO HTTPResponseDecoder",
        "NIO HTTPRequestHeadersValidator",
        "Whooshing Request Wrapper Handler"
    ]
    
    @inlinable
    public required init(eventLoop: EventLoop, logger: Logger? = nil, byteBufferAllocator: ByteBufferAllocator, ioHandler: IOHandler? = nil) {
        self.eventLoop = eventLoop
        self.fileEventLoop = eventLoop.next()
        self.logger = logger
        self.byteBufferAllocator = byteBufferAllocator
        self.ioHandler = ioHandler
    }
}
 
extension ReqClient {
    
    @frozen
    public enum Errcase: String, ErrList {
        case requestFormatError = "请求格式有误"
        case requestBodyTooLarge = "请求的内容过大"
        case requestParseFailed = "服务器响应头解包时出错"
        case requestDomainParseFailed = "域名解析失败"
        case tcpHandlerInitialFailed = "TCP 中间流处理器初始化失败"
        case tcpHandlerRemoveFailed = "TCP 中间流处理器移除失败"
        case tcpSocketConnectFailed = "TCP 连接失败"
        case tcpSendFailed = "TCP 通道写入数据失败"
        case tcpHandlerFailed = "TCP 中间流处理器处理失败"
    }

    public func makeChannel(url: WebURI) -> EventLoopRes<(Channel, RequestWrapperHandler, domain: String?), Errcase> {
        
        guard [.http, .https].contains(url.scheme) else {
            return eventLoop.makeFailedResult(Errcase.requestFormatError, "无效协议", metadata: ["schema": .data(url)], category: .external(suggestions: ["预期请求协议为 http 或 https"]))
        }

        let port: Int
        let isDomainHost = url.isDomainHost()
        if isDomainHost {
            port = url.port ?? (url.scheme == .https ? 443 : 20002)
        } else {
            guard let p = url.port else {
                return eventLoop.makeFailedResult(Errcase.requestFormatError, "未找到目标端口号", metadata: ["schema": .data(url)], category: .external(suggestions: ["请使用 ip:port 格式指定目标端口号"]))
            }
            port = p
        }
        
        let cryptoHandler = RequestCryptoHandler(logger: logger?.derive(subId: "handler.crypto"), ioHandler: ioHandler)
        let wrapperHandler = RequestWrapperHandler(logger: logger?.derive(subId: "handler.wrapper"))

        let bootstrap = ClientBootstrap(group: self.eventLoop)
            .channelInitializer { channel in
                channel.pipeline.addHandlers([
                    NIOCloseOnErrorHandler(),
                    RequestBackPressureHandler(logger: self.logger?.derive(subId: "handler.backpressure")),
                    LengthFieldPrepender(lengthFieldLength: .eight, lengthFieldEndianness: .big),
                    ByteToMessageHandler(LengthFieldBasedFrameDecoder(lengthFieldLength: .eight, lengthFieldEndianness: .big))
                ]).flatMap {
                    channel.eventLoop.bridge {
                        let handlers: [ChannelHandler] = [
                            cryptoHandler,
                            HTTPRequestEncoder(configuration: .init()),
                            ByteToMessageHandler(HTTPResponseDecoder(leftOverBytesStrategy: .dropBytes)),
                            NIOHTTPRequestHeadersValidator(),
                            wrapperHandler
                        ]
                        for (i, handler) in handlers.enumerated() {
                            try await channel.pipeline.addHandler(handler, name: self.removableHandlerNames[i])
                        }
                    }
                }
            }
            .channelOption(.socketOption(.tcp_nodelay), value: 1)
            .channelOption(.socketOption(.so_reuseaddr), value: 1)
            .channelOption(.maxMessagesPerRead, value: 1)
            .channelOption(.autoRead, value: false)

        return bootstrap.connect(host: url.host, port: port).map { channel in
            self.__channel = channel
            return (channel, wrapperHandler, isDomainHost ? url.host : nil)
        }.withError(Errcase.tcpHandlerInitialFailed, category: .internal)
    }

    public func send(
        _ client: HTTPRequest,
        channel: Channel,
        handler: RequestWrapperHandler
    ) -> EventLoopRes<HTTPResponse, Errcase> {
        let promise: EventLoopTarget<HTTPResponse, RequestWrapperHandler.Errcase.ErrType> = channel.eventLoop.makeTarget(of: HTTPResponse.self)
        handler.promise = promise
        return channel.writeAndFlush(client)
            .withError(Errcase.tcpSendFailed, category: .inherit)
            .flatMapError
        { err in
            self.logger?.warning("\(err)")
            promise.fail(RequestWrapperHandler.Errcase.cancelled.d(category: .internal))
            return channel.eventLoop.makeFailedResult(err)
        }.flatMap {
            promise.futureResult.errCast(Errcase.tcpHandlerFailed, category: .inherit)
        }
    }

    public func close() async {
        guard let channel = channel else {
            self.logger?.info("通道未建立，无需关闭请求连线")
            return
        }
        
        self.logger?.info("正在关闭请求连线", metadata: ["channel_client_addr": .string(channel.clientAddrInfo)])
        try? await channel.close(mode: .all)
    }
    
    public func removeHTTPHandlers(in eventLoop: any EventLoop) -> EventLoopRes<Void, Errcase> {
        eventLoop.bridge { () throws(Errcase.ErrType) in
            try await self.removeHTTPHandlers().get()
        }
    }
    
    public func removeHTTPHandlers() async -> Res<Void, Errcase> {
        guard let channel = self.channel else { return .success(()) }
        for name in self.removableHandlerNames {
            do {
                self.logger?.debug("正在移除 Handler", metadata: ["handler_name": .string(name), "client_addr": .string(channel.clientAddrInfo)])
                try await required(throws: Errcase.tcpHandlerRemoveFailed, category: .inherit) {
                    try await channel.pipeline.removeHandler(name: name)
                }
            } catch {
                return .failure(error)
            }
        }
        return .success(())
    }
}
