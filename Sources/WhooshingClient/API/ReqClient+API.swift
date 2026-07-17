import NIOAdvanced
import Cryptos
import NIOHTTP1
import Foundation
import AsyncHTTPClient
import LoggingAdvanced

@usableFromInline
final class APIReqClient: ReqClient<API.RequestIOCrypto>, SendableStorage.Key, @unchecked Sendable {
    @usableFromInline
    typealias Value = APIReqClient
    @usableFromInline
    typealias Errcase = ApiClient.Errcase
    @usableFromInline
    typealias Failure = ApiClient.Failure

    @inlinable
    static func new(eventLoop: EventLoop, logger: Logger? = nil, byteBufferAllocator: ByteBufferAllocator) -> Self {
        let res = Self(eventLoop: eventLoop, logger: logger, byteBufferAllocator: byteBufferAllocator)
        res.ioHandler = API.RequestIOCrypto(client: res)
        return res
    }
    
    @inlinable
    func send(
        _ request: HTTPRequest
    ) -> EventLoopResult<HTTPResponse, Failure> {
        self.makeChannel(url: request.url)
            .errCast(Errcase.tcpChannelAssignFailed, category: .inherit)
            .flatMap
        { channel, handler, domain in
            self.logger?.info("API.Client-发送请求", metadata: ["client_addr": .string(channel.clientAddrInfo)])
            self.logger?.debug("请求内容", metadata: ["request": .data(request)])
            return self._send(request: request, channel: channel, handler: handler, domain: domain).map { res in
                self.logger?.info("发送请求成功，收到响应", metadata: ["status": .stringConvertible(res.status)])
                self.logger?.debug("响应内容", metadata: ["response": .data(res)])
                return res
            }
        }.logIfFailAndExist(logger: self.logger)
    }

    struct JSONData: Codable {
        let data: Data
    }
    
    @usableFromInline
    func _send(
        request: HTTPRequest,
        channel: Channel,
        handler: RequestWrapperHandler,
        domain: String?
    ) -> EventLoopResult<HTTPResponse, Failure> {
        var r = eventLoop.makeSucceededVoidResult(throws: Failure.self)
        guard let ioData = self.apiRequestIoData else {
            return eventLoop.makeFailedResult(Errcase.internalFailure, "请求参数缺失: apiRequestIoData", category: .internal)
        }
        if ioData.connectionKey == nil {
            self.logger?.debug("正在与服务器进行认证", metadata: ["client_addr": .string(channel.clientAddrInfo)])
            r = r.flatMap {
                self.authExchange(request: request, handler: handler, domain: domain, channel: channel)
            }
        }
        return r.flatMap{
            self.logger?.debug("正在与服务器发送具体的请求", metadata: ["client_addr": .string(channel.clientAddrInfo), "request": .data(request)])
            return self.send(request, channel: channel, handler: handler).errCast(Errcase.tcpSendFailed, "发送用户请求失败", category: .inherit)
        }
    }

    struct AuthExchangeJSON: Encodable {
        let credential: Data
        let tokenEncrypted: Data
        
        enum CodingKeys: String, CodingKey {
            case credential
            case tokenEncrypted = "token_encrypted"
        }
    }

    /// 发送用户凭据以及用户口令，其中用户凭据明文发送，口令则进行哈希加密：密文 = [口令加密[口令 hash]]
    func authExchange(
        request: HTTPRequest,
        handler: RequestWrapperHandler,
        domain: String?,
        channel: Channel
    ) -> EventLoopResult<Void, Failure> {
        channel.eventLoop.submitResult { () throws(Failure) in
            guard let ioData = self.apiRequestIoData else {
                throw Errcase.internalFailure.d("apiRequestIoData 参数未找到", category: .internal)
            }
            
            self.logger?.debug("取得 ioData", metadata: ["io_data": .data(ioData)])
            
            guard let credential = Data(base64Encoded: ioData.credential) else {
                throw Errcase.badRequest.d("解析请求中的 用户凭据 数据失败", category: .external(suggestions: ["请检查用户凭据是否正确提供"]))
            }
            
            self.logger?.debug("API.Client-认证中: 使用用户口令加密用户口令的 hash")
            guard let token = Data(base64Encoded: ioData.token) else {
                throw Errcase.badRequest.d("解析请求中的 用户口令 数据失败", category: .external(suggestions: ["请检查用户口令是否正确提供"]))
            }
            
            let tokenKey = Crypto.Symm.Key(data: token)
            let tokenEncrypted = try required(throws: Errcase.encryptFailed, "对密钥 hash 进行对称加密失败", category: .internal) {
                try Crypto.Symm.encrypt(Crypto.hash(token), key: tokenKey).get()
            }
            
            self.logger?.debug("API.Client-认证中: 将凭据和加密后的用户口令进行 json 编码")
            let body = try required(throws: Errcase.jsonEncodeFailed, category: .internal) {
                try HTTPBody.json(AuthExchangeJSON(credential: credential, tokenEncrypted: tokenEncrypted)).get()
            }
            
            self.logger?.debug("API.Client-认证中: 发送用户凭据以及用户口令")
            var headers: HTTPHeaders = ["content-type": "application/json"]
            if let domain = domain {
                headers.replaceOrAdd(name: "host", value: domain)
            }
            
            return (
                ioData,
                HTTPRequest(
                    method: .POST,
                    url: request.url,
                    headers: headers,
                    body: body
                )
            )
        }.flatMap { (ioData: API.RequestIOData, req) in
            self.send(req, channel: channel, handler: handler).errCast(Errcase.tcpSendFailed, category: .inherit).map { (ioData, $0) }
        }.flatMapThrowing { ioData, res throws(Failure) in
            self.logger?.debug("API.Client-正在完成认证: 认证请求发送完成", metadata: ["result": .data(res)])
            guard res.status == .ok else {
                throw Errcase.badResponse.d("身份认证失败", category: .internal).metadata(["status": .stringConvertible(res.status)])
            }
            
            guard let resBody = res.body else {
                throw Errcase.badResponse.d("响应体为空", category: .internal)
            }
            
            // 当向认证模块发送认证请求之后，应当得到一个使用用户口令加密的新密钥，并使用该新密钥进行后续的通讯加密
            self.logger?.debug("API.Client-正在完成认证: 解析服务器的新密钥")
            guard let token = Data(base64Encoded: ioData.token) else {
                throw Errcase.badResponse.d("未解析得到用户口令", category: .internal)
            }
            let tokenKey = Crypto.Symm.Key(data: token)
            
            self.logger?.debug("API.Client-正在完成认证: 获取对方发来的加密新密钥")
            let keyEncrypted = try required(throws: Errcase.jsonDecodeFailed, category: .internal) {
                try resBody.json(as: JSONData.self).get().data
            }
            
            self.logger?.debug("API.Client-正在完成认证: 使用用户口令解密新密钥")
            let newKey: SendableSymmKey = try required(throws: Errcase.decryptFailed, category: .internal) {
                try Crypto.Symm.decrypt(keyEncrypted, key: tokenKey).get()
            }
            
            self.logger?.debug("API.Client-正在完成认证: 注册该新密钥，用于将来的连线加密")
            ioData.connectionKey = newKey
        }
    }

    deinit {
        Task { [weak self] in
            await self?.close()
        }
    }
}
