import Cryptos
import NIOAdvanced
import Dispatch
import AsyncHTTPClient

/// 一个用于发起 API 子模块请求的客户端类，封装了鉴权和请求配置。
/// 并自动将用户凭据与令牌存储在请求上下文中，用于后续认证
///
/// 实现了数据流发送的 Backpressure 机制，因此尽管大数据流发送也不会产生内存堆积和泄漏的问题。
/// 该类型的所有接口见协议 `WhooshingClient`
///
/// - Warning: 该请求类型使用了 Whooshing 自定加密，因此无法访问任何常规 HTTP 或 HTTPS
/// 网站服务，仅应当用于访问 WHooshing 系统的 API 服务子模块。请勿使用 HTTPS 协议请求连线
/// ，使用 HTTP 即可。
public final class ApiClient: Sendable {
    
    @inlinable
    public var key: SendableSymmKey? {
        guard
            let ioData = client.storage[API.RequestIOData.self],
            let _ = client.channel,
            let key = ioData.connectionKey
        else { return nil }
        return key
    }
    
    /// 当前正在进行的请求所在的 TCP NIO Channel
    @inlinable
    public weak var channel: (any Channel)? { client.channel }
    /// 日志系统
    @inlinable
    public var logger: Logger? { client.logger }
    /// 主要的 EventLoop
    @inlinable
    public var eventLoop: EventLoop { client.eventLoop }
    
    @usableFromInline
    let client: APIReqClient
    @usableFromInline
    let allocator = ByteBufferAllocator()
    
    /// 使用指定的事件循环和日志器初始化 API 客户端。
    ///
    ///
    /// - Parameters:
    ///   - credential: 用户凭据（Base64 编码的字符串）。
    ///   - token: 用户令牌（Base64 编码的字符串）。
    ///   - eventLoop: 所属的事件循环，用于异步操作。
    ///   - logger: 可选的日志记录器，默认值为 `nil`。
    @inlinable
    public init(credential: String, token: String, eventLoop: EventLoop, logger: Logger? = nil) {
        self.client = .new(eventLoop: eventLoop, logger: logger, byteBufferAllocator: allocator)
        client.storage[API.RequestIOData.self] = .init(credential: credential, token: token)
    }
    
    /// 关闭所有正在进行的连线
    @inlinable
    public func shutdown() async throws {
        logger?.info("API.Client-主动关闭连接", metadata: ["client_addr": .stringConvertible(channel?.clientAddrInfo ?? "released")])
        await client.close()
    }
    
    @inlinable
    public func syncShutdown() throws {
        let semaphore = DispatchSemaphore(value: 0)
        let errorBox = Box()
        Task.detached {
            do {
                try await self.shutdown()
            } catch {
                errorBox.error = error
            }
            semaphore.signal()
        }
        
        semaphore.wait()
        
        if let e = errorBox.error { throw e }
    }

    @inlinable
    deinit {
        client.logger?.debug("API.Client-主动关闭连接")
        Task { [weak client] in
            await client?.close()
        }
    }
}

/// 实现 WhooshingClient 协议，以继承其默认实现
extension ApiClient: WhooshingClient {
    @inlinable
    public func removeHTTPHandlers(in eventLoop: any EventLoop) -> EventLoopResult<Void, Failure> {
        self.client.removeHTTPHandlers(in: eventLoop)
            .errCast(Errcase.tcpHandlerRemoveFailed, category: .inherit)
    }
    
    @inlinable
    public func removeHTTPHandlers() async -> Result<Void, Failure> {
        await self.client.removeHTTPHandlers().mapError(as: Errcase.tcpHandlerRemoveFailed, category: .inherit)
    }
    
    @inlinable
    public func send(
        _ request: HTTPRequest
    ) -> EventLoopResult<HTTPResponse, Failure> {
        client.send(request)
    }

    @inlinable
    public var fileEventLoop: any EventLoop { client.fileEventLoop }
}

public final class Box: @unchecked Sendable {
    public var error: Error?
    public init(error: Error? = nil) {
        self.error = error
    }
}
