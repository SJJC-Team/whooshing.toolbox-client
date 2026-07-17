import Cryptos
import NIOHTTP1
import NIOAdvanced
import Foundation
import AsyncHTTPClient
import AnyCodable
import LoggingAdvanced
import NIOConcurrencyHelpers

extension APIReqClient {
    @inlinable
    var apiRequestIoData: API.RequestIOData? { self.storage[API.RequestIOData.self] }
}

@usableFromInline
enum API {
    @frozen
    public enum Errcase: String, ErrList {
        case requestEncryptFailed = "请求数据加密失败"
        case responseDecryptFailed = "响应数据解密失败"
        case responseParseErrorFailed = "从响应数据解析错误信息时失败"
        case parseResponseFailed = "解析响应数据时失败"
        case internalFailure = "内部错误"
    }
    
    @usableFromInline
    final class RequestIOData: SendableStorage.Key, @unchecked Sendable, CustomStringConvertible, Loggerable {
        @usableFromInline typealias Value = RequestIOData
        @usableFromInline let credential: String
        @usableFromInline let token: String
        
        @usableFromInline var connectionKey: SendableSymmKey? {
            get { lock.withLock { _connectionKey } }
            set { lock.withLock { _connectionKey = newValue } }
        }
        @usableFromInline var readingBufferData: ByteBuffer? {
            get { lock.withLock { _readingBufferData } }
            set { lock.withLock { _readingBufferData = newValue } }
        }
        @usableFromInline var errorTemp: Bool? {
            get { lock.withLock { _errorTemp } }
            set { lock.withLock { _errorTemp = newValue } }
        }
        
        var _connectionKey: SendableSymmKey?
        var _readingBufferData: ByteBuffer?
        var _errorTemp: Bool?
        
        private let lock = NIOLock()
        
        @usableFromInline
        init(credential: String, token: String) {
            self.credential = credential
            self.token = token
        }
        
        @inlinable
        public var description: String {
            formatJson([
                "credential": AnyCodable(credential)
            ])
        }
    }
    
    @usableFromInline
    struct RequestIOCrypto: RequestCryptoIOHandler, Sendable {
        
        @usableFromInline
        typealias Failure = Errcase.ErrType
        
        @usableFromInline
        weak private(set) var client: APIReqClient?
        
        @inlinable
        var isAvaliable: Bool { client?.apiRequestIoData != nil }
        
        @inlinable
        init(client: APIReqClient? = nil) {
            self.client = client
        }
        
        /// 发送请求时，进行编码并加密
        @usableFromInline
        func send(data: ByteBuffer, context: ChannelHandlerContext, logger: Logger?) -> EventLoopRes<ByteBuffer, Errcase> {
            logger?.debug("API.Client.HTTP-发送请求，进行加密", metadata: ["client_addr": .string(context.channel.clientAddrInfo)])
            guard data.readableBytes > 0 else {
                logger?.debug("请求数据为空，忽略")
                return context.eventLoop.makeSucceededResult(data)
            }
            guard let ioData = client?.apiRequestIoData else { return context.eventLoop.makeFailedResult(Errcase.internalFailure.d("apiRequestIoData", category: .internal)) }
            do {
                let cipher: Data
                if let key = ioData.connectionKey {
                    logger?.debug("使用已有密钥加密通讯")
                    cipher = try required(throws: Errcase.requestEncryptFailed, category: .internal) {
                        try Crypto.Symm.encrypt(data, key: key.key).get()
                    }
                } else {
                    logger?.debug("首次请求，直接发送明文凭据", metadata: ["data": .stringConvertible(data)])
                    // 代表首次请求，直接发送明文
                    // 用户凭据可明文发送，且用户口令会加密处理，因此整个请求无需加密
                    cipher = .init(buffer: data)
                }
                let buffer = ByteBuffer(data: cipher)
                return context.eventLoop.makeSucceededResult(buffer)
            } catch let err {
                return context.eventLoop.makeFailedResult(err)
            }
        }
        
        @usableFromInline
        func get(data: ByteBuffer, context: ChannelHandlerContext, logger: Logger?) -> EventLoopRes<ByteBuffer, Errcase> {
            logger?.debug("API.Client.HTTP-收到响应，先检查是否有报错", metadata: ["client_addr": .string(context.channel.clientAddrInfo)])
            
            guard data.readableBytes > 0 else {
                logger?.debug("响应数据为空，忽略")
                return context.eventLoop.makeSucceededResult(data)
            }
            guard let ioData = client?.apiRequestIoData else { return context.eventLoop.makeFailedResult(Errcase.internalFailure.d("apiRequestIoData 读取失败", category: .internal)) }
            
            // 检查对方回复的是不是一个未加密的 http 回复，如果是，则表示对方出错
            if let _ = ioData.errorTemp {
                // 如果错误已经存在了，则直接报错
                let err = parseError(body: data)
                ioData.errorTemp = nil
                return context.eventLoop.makeFailedResult(err)
            } else {
                // 错误不存在，从对方的响应中尝试解析出错误
                if lightweightParseHTTP1StatusCode(from: data) {
                    ioData.errorTemp = true
                    return context.eventLoop.makeSucceededResult(data)
                }
            }
                
            do {
                logger?.debug("API.Client.HTTP-收到响应，进行解密", metadata: ["client_addr": .string(context.channel.clientAddrInfo)])
                var plain: ByteBuffer
                if let key = ioData.connectionKey {
                    logger?.debug("使用已有密钥进行解密")
                    plain = try required(throws: Errcase.responseDecryptFailed, category: .internal) {
                        try Crypto.Symm.decrypt(.init(buffer: data), key: key.key).get()
                    }
                } else {
                    logger?.debug("首次得到响应，使用默认密钥分析响应")
                    guard let token = Data(base64Encoded: ioData.token) else {
                        throw Errcase.parseResponseFailed.d("用户口令", category: .internal)
                    }
                    let tokenKey = Crypto.Symm.Key(data: token)
                    plain = try required(throws: Errcase.responseDecryptFailed, category: .internal) {
                        try Crypto.Symm.decrypt(.init(buffer: data), key: tokenKey).get()
                    }
                }
                return context.eventLoop.makeSucceededResult(plain)
            } catch let error as Errcase.ErrType {
                return context.eventLoop.makeFailedResult(error)
            } catch {
                return context.eventLoop.makeFailedResult(Errcase.internalFailure.subErr(error, category: .internal))
            }
        }
        
        // 连线建立
        @usableFromInline
        func connectionStart(context: ChannelHandlerContext, logger: Logger?) -> EventLoopRes<Void, Errcase> {
            logger?.debug("API.Client-连线建立", metadata: ["client_addr": .string(context.channel.clientAddrInfo)])
            return context.eventLoop.makeSucceededVoidResult()
        }

        // 连线结束，进行清理
        @usableFromInline
        func connectionEnd(context: ChannelHandlerContext, logger: Logger?) -> EventLoopRes<Void, Errcase> {
            logger?.debug("API.Client-连线结束", metadata: ["client_addr": .string(context.channel.clientAddrInfo)])
            client?.apiRequestIoData?.connectionKey = nil
            client?.apiRequestIoData?.readingBufferData = nil
            return context.eventLoop.makeSucceededVoidResult()
        }

        // 检查 response 是否为 HTTP 格式的头，如果是，则返回其状态码
        func checkHeader(res: ByteBuffer) -> HTTPResponseStatus? {
            guard let res = try? String.make(data: res.data).get() else {
                return nil
            }
            let lines = res.split(separator: "\r\n")
            let fields = lines[0].split(separator: " ")
            if fields.count >= 3, let code = Int(fields[1]) {
                return .init(statusCode: code)
            }
            
            return nil
        }

        struct BodyReply: Codable {
            @usableFromInline
            let error: Bool
            @usableFromInline
            let reason: String
        }
        
        // 检查 response 是否为 HTTP 格式且包括错误状态码
        func parseError(body: ByteBuffer) -> Errcase.ErrType {
            let reply: BodyReply
            do {
                reply = try required(throws: Errcase.responseParseErrorFailed, "应当解析出 Error 信息", category: .internal) {
                    try JSONDecoder().decode(BodyReply.self, from: Data(buffer: body))
                }
            } catch let err {
                return err
            }
                
            if reply.error {
                return Errcase.internalFailure.d(reply.reason, category: .internal)
            } else {
                return Errcase.responseParseErrorFailed.d("应当解析出 Error 信息，但失败", category: .internal)
            }
        }
        
        func lightweightParseHTTP1StatusCode(from buffer: ByteBuffer) -> Bool {
            // 确保至少有 "HTTP" 这段内容（4 字节）
            guard buffer.readableBytes > 4 else {
                return false
            }

            // 获取前 4 个字节，应该是 "HTTP"
            let expectedPrefix: [UInt8] = [0x48, 0x54, 0x54, 0x50] // "HTTP" in ASCII

            if let view = buffer.getBytes(at: buffer.readerIndex, length: 4) {
                return view.elementsEqual(expectedPrefix)
            } else {
                return false
            }
        }
    }
}
