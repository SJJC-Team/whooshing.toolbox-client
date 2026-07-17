# Whooshing 客户端请求库

WhooshingClient 是 Whooshing 系统中的通用网络请求库，具备同步封装、加密能力、Backpressure 机制、模块化设计等特性。可用于加密访问 Whooshing 系统的服务模块

该库已集成于 API、INLINE、HTTPS 三大子模块，并可作为服务内部通信、远程调用、数据传输的统一方案。

关于模块与子模块，见 [whooshing.toolbox-server](https://github.com/whooshing-workshop/whooshing.toolbox-server)

---------

### 特性

-  **统一协议定义**：通过 WhooshingClient 协议定义 GET/POST/PUT/PATCH/DELETE 等标准方法，支持自定义请求。
-  **异步 & 并发支持**：提供 async/await 封装，无需关注底层 EventLoop。
-  **模块隔离**：支持 ApiClient、HttpsClient 两种通信客户端，职责明确。
-  **加密通信**：API 模块集成 Whooshing 自定义加密机制，保障数据安全。
-  **Backpressure 控制**：请求发送支持流式数据调节，防止内存堆砌和溢出。
-  **流式处理**：支持 bytes 与 stream 类型的请求体，适用于文件上传等场景。

----

### 模块说明

- **ApiClient:** 用于访问任意 Whooshing API 子模块，需要提供用户认证信息
- **HttpsClient:** 用于访问任意 Whooshing HTTPS 子模块

关于模块与子模块，见 [whooshing.toolbox-server](https://github.com/whooshing-workshop/whooshing.toolbox-server)

-----------

### 导入该依赖库

在你的 Package.swift 加入：

``` swift
.package(url: "https://github.com/whooshing-workshop/whooshing.toolbox-client.git", from: "1.3.3")
```

在依赖模块中引入:

```swift
.product(name: "WhooshingClient", package: "whooshing.toolbox-client")
```

在需要的地方:

```swift
import WhooshingClient
```

--------

### 使用介绍

##### 创建 Client

对于 **ApiClient**，需要提供用户认证信息，包括用户凭据和用户密钥。关于认证机制，见 [whooshing.system-authentication](https://github.com/whooshing-workshop/whooshing.system-authentication)

```swift
let client = ApiClient(credential: "bRRPIiYbt0t4RzfqeeHSkg==", token: "jXTz4vTQk0O/XFIjWQIHLC7z9/E0/4VtEb+LkF8IcA4=", eventLoop: eventLoop, logger: logger)
```

对于 **HttpsClient**，无需提供额外信息

```swift
let client = HttpsClient(in: eventLoop, logger: logger)
```

> 创建一个 client 时，eventLoop 是必须的，可见 [SwiftNIO](https://github.com/apple/swift-nio) 对 EventLoop 的文档
>
> 一般来说，你可以简单地创建一个以系统核心数创建的线程池：
>
> ```swift
> import NIOCore
> 
> let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
> let eventLoop = eventLoopGroup.next()
> ```
>
> logger 是可选的，推荐为其设置 logger



##### 执行常规 HTTP 请求(GET/POST/PUT/PATCH/DELETE)

一个简单的 get 请求，发送至 http://localhost:6502

```swift
let res: HTTPResponse = try await client.get("http://localhost:6502")
print(res)
```

你也可以使用 NIO 的非阻塞模型

```swift
client.get("http://localhost:6502").whenSuccess { res
	print(res)
}
```

也可以使用其他的 HTTP 请求方法:

```swift
let getRes: HTTPResponse = try await client.get("http://localhost:6502")
let postRes: HTTPResponse = try await client.post("http://localhost:6502")
let putRes: HTTPResponse = try await client.put("http://localhost:6502")
let patchRes: HTTPResponse = try await client.patch("http://localhost:6502")
let deleteRes: HTTPResponse = try await client.delete("http://localhost:6502")
```

或：

```swift
let sendRes: HTTPResponse = try await client.send(.GET, to: "http://localhost:6502")
```



##### HTTP 数据交互与流式传输 - 为 HTTPRequest 编码数据

你可以将一段文字编码至一个 HTTP 请求的请求体中：

```swift
let res = try await client.get("http://localhost:6502", body: .text("Hello World!"))
```

> 这将自动为该 HTTP 请求头设置 Content-Type = plain/text , 且 Content-Length = 12

或者一段数据:

```swift
let data: Data = .........
let res = try await client.get("http://localhost:6502", body: .data(data))
```

> HTTPBody 支持编码多种形式的数据，具体请见 [HTTPBody+Encode.swift](Sources/WhooshingClient/Extensions/HTTPBody+Encode.swift)

HTTPRequest 或 HTTPResponse 的 body 也支持使用 AsyncThrowingChannel 创建流式传输：

```swift
import AsyncAlgorithms

let stream = AsyncThrowingChannel<ByteBuffer, Error>()
// 创建异步执行，应避免在主线程进行数据读取的耗时操作
Task {
    for ctx in Progress(pieces: 10, chunk: 1024) {
        let data = ............
        await stream.send(data)
    }
    stream.finish()
}
let res = try await client.get("http://localhost:6502/streaming", body: .stream(stream))
```

这里，`Progress(pieces: 10, chunk: 1024)` 用于创建一个数据发送任务，依次调用 `AsyncThrowingChannel` 的 `send` 发送数据块，最后调用 `finish` 标志流结束。

> 不要忘记 `import AsyncAlgorithms`
>
> 数据仅会在 TCP 底层准备好发送新数据时从 stream 中读取，因此不必担心造成内存泄漏或堆砌。这也是为什么 `stream.send(data)` 是 `await` 的，即，它可能阻塞当前线程
>
> 你可以直接 `print(ctx)` 打印出最直观的进度信息。关于 Progress 的用法，见 [Progress.swift](Sources/WhooshingClient/Basic/Progress.swift)

HTTPBody 并不仅仅支持类型为 ByteBuffer 的数据块，例如，我们可以每次发送一个字符串数据：

```swift
import AsyncAlgorithms

let stream = AsyncThrowingChannel<String, Error>()
// 创建异步执行，应避免在主线程进行数据读取的耗时操作
Task {
    for ctx in Progress(pieces: 10, chunk: 1024) {
        await stream.send("Hello World!")
    }
    stream.finish()
}
let res = try await client.get("http://localhost:6502/streaming", body: .stream(stream))
```

你甚至可以发送一个 Encodable 的结构体:

```swift
import AsyncAlgorithms

struct StreamDataChunk: Encodable {
    let name: String
    let age: Int
    let description: String
}

let stream = AsyncThrowingChannel<StreamDataChunk, Error>()
// 创建异步执行，应避免在主线程进行数据读取的耗时操作
Task {
    for ctx in Progress(pieces: 10, chunk: 1024) {
        await stream.send(.init(name: "ChenLin Wang", age: 24, description: "XXXXXX"))
    }
    stream.finish()
}
let res = try await client.get("http://localhost:6502/streaming", body: .jsonStream(stream))
```

> 关于详细的 Body Encode 功能，见 [HTTPBody+Encode.swift](Sources/WhooshingClient/Extensions/HTTPBody+Encode.swift)
>
> 另外，你也可以发起一个文件流，从文件系统中读取文件数据并发给对方，此处不多解释，见 [HTTPBody+Encode.swift](Sources/WhooshingClient/Extensions/HTTPBody+Encode.swift)



##### HTTP 数据交互与流式传输 - 从 HTTPResponse 解码数据

自然，你也可以从 `HTTPResponse` 中解出所预期的值，例如，从对方发来的响应中得到一串字符串：

```swift
let res = try await client.get("http://localhost:6502")
guard let body = res.body else { fatalError("对方没有返回任何数据") }
let text: String = try body.text()
print(text)
```

> 所返回的 response，其 body 可能为 nil，这表示对方并未发送任何数据，你需要进行一些判断逻辑
>
> `body.text()` 可能抛错，因为其 body 可能为其他类型的数据而导致解码失败 

或者解析出一个数据包:

```swift
let res = try await client.get("http://localhost:6502")
guard let body = res.body else { fatalError("对方没有返回任何数据") }
let data: Data = try body.data()
print(data.count)
```

> HTTPBody 支持解码为多种形式的数据，具体请见 [HTTPBody+Decode.swift](Sources/WhooshingClient/Extensions/HTTPBody+Decode.swift)

若对方发来的响应是以流式传输数据的：

```swift
let res = try await client.get("http://localhost:6502")
guard let body = res.body else { fatalError("对方没有返回任何数据") }
let stream = try body.stream(as: String.self)
for try await chunk in stream {
    print("数据块: \(chunk)")
}
```

 表示你将会等待对方的流式传输，且依次处理对方发来的数据。此例表示从流中解包出 String 数据块。

或者，你也可以选择解析为 `Data`:

```swift
let stream = try body.stream(as: Data.self)
```

甚至你自己的自定类型，需要实现 `Decodable`:

```swift
struct StreamDataChunk: Decodable {
    let name: String
    let age: Int
    let description: String
}

let res = try await client.get("http://localhost:6502")
guard let body = res.body else { fatalError("对方没有返回任何数据") }
let stream = try body.jsonStream(as: StreamDataChunk.self)
for try await chunk in stream {
    print("该数据块的名称: \(chunk.name), 年龄: \(chunk.age), 介绍: \(chunk.description)")
}
```

你也可以使用 `withProgress()` 轻松地为数据读取加上进度信息：

```swift
let res = try await client.get("http://localhost:6502")
guard let body = res.body else { fatalError("对方没有返回任何数据") }
let stream = try body.jsonStream(as: String.self)
for try await (ctx, chunk) in stream.withProgress() {
    print("当前进度: \(ctx)")
    print("数据块: \(chunk)")
}
```

> 关于 `withProgress()` 的用法，见 [AsyncChannel+Progress.swift](Sources/WhooshingClient/Extensions/AsyncChannel+Progress.swift)

你可以在读取数据时进行耗时操作，例如：

```swift
for try await (ctx, chunk) in stream.withProgress() {
    print("当前进度: \(ctx)")
    print("数据块: \(chunk)")
    sleep(10)		// 将当前线程阻塞 10 秒
}
```

如此这般，底层 TCP 也会同时暂停接收对方发来的任何数据，直到此处的耗时操作完成。因此无论对方发来的数据总体有多大或网络速度如何，都不会造成内存泄漏或堆砌。

> 另外，你也可以从流中解析出对方发来的文件数据，并自动存入文件系统。此处不多解释，见 [HTTPBody+Decode.swift](Sources/WhooshingClient/Extensions/HTTPBody+Decode.swift)

---------

### 运行环境

* **macOS** (> 10.15)
* **iOS** (> 14.0)
* **Linux** (> 20)
* **Swift** (> 6.0)
* **watchOS** (> 6.0) **[未测试]**
* **tvOS**(> 13) **[未测试]**

---------

### 注意事项

- **ApiClient** 仅可用于访问 Whooshing 的 API 模块，不可用于外部服务，由于其有自定加密，永远不应当使用 HTTPS。
- **HttpsClient** 使用传统的网络加密，因此务必使用 HTTPS 进行安全访问，避免 HTTP 明文发送。

如需了解更多，请参阅各模块内的源码注释与文档说明。

-------

### 联系与反馈

如有使用问题或建议，请通过 [GitHub Issues](https://github.com/whooshing-workshop/whooshing.toolbox-client/issues) 提交反馈。

或发至邮箱 [contact@official.whooshings.space](mailto:contact@official.whooshings.space)
