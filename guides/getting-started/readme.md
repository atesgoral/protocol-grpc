# Getting Started

This guide explains how to use `protocol-grpc` for building abstract gRPC interfaces.

## Installation

Add the gem to your project:

~~~ bash
$ bundle add protocol-grpc
~~~

## Core Concepts

`protocol-grpc` has several core concepts:

  - A {ruby Protocol::GRPC::Interface} class which defines gRPC service contracts with RPC methods, request/response types, and streaming patterns.
  - A {ruby Protocol::GRPC::Body::Readable} class which handles reading gRPC messages from HTTP request/response bodies with automatic framing and decoding.
  - A {ruby Protocol::GRPC::Body::Writable} class which handles writing gRPC messages to HTTP request/response bodies with automatic framing and encoding.
  - A {ruby Protocol::GRPC::Middleware} abstract base class for building gRPC server applications.
  - A {ruby Protocol::GRPC::Call} class which represents the request, response, metadata, and deadline for a single gRPC RPC call.
  - A {ruby Protocol::GRPC::Status} module with gRPC status code constants and names.
  - A {ruby Protocol::GRPC::Error} hierarchy for gRPC-specific error handling.

## Integration

This gem provides protocol-level abstractions only. To actually send requests over the network, you need an HTTP/2 client/server implementation:

  - [Async::GRPC](https://github.com/socketry/async-grpc) which provides asynchronous client and server implementations.
  - [Async::HTTP](https://github.com/socketry/async-http) which provides HTTP/2 transport with connection pooling and concurrency.

## Usage

### Defining an Interface

{ruby Protocol::GRPC::Interface} defines the contract for gRPC services. RPC method names use PascalCase to match `.proto` files:

``` ruby
require "protocol/grpc/interface"

class GreeterInterface < Protocol::GRPC::Interface
	# Unary RPC (single request, single response)
	rpc :SayHello, Hello::HelloRequest, Hello::HelloReply
	
	# Server streaming RPC using stream() decorator
	rpc :SayHelloMany, Hello::HelloRequest, stream(Hello::HelloReply)
	
	# Client streaming RPC
	rpc :SayHelloRepeatedly, stream(Hello::HelloRequest), Hello::HelloReply
	
	# Bidirectional streaming RPC
	rpc :ChatHello, stream(Hello::HelloRequest), stream(Hello::HelloReply)
end
```

The `stream()` decorator marks message types as streamed. You can also use the keyword syntax:

``` ruby
rpc :SayHelloAgain, request_class: Hello::HelloRequest, response_class: Hello::HelloReply,
	streaming: :server_streaming
```

### Building a Request

Build gRPC requests using `Protocol::GRPC::Metadata`, `Protocol::GRPC::Route`, and `Protocol::GRPC::Body::Writable`:

``` ruby
require "protocol/grpc"
require "protocol/grpc/body/writable"

# Build request body
body = Protocol::GRPC::Body::Writable.new(message_class: Hello::HelloRequest)
body.write(Hello::HelloRequest.new(name: "World"))
body.close_write

# Build headers
headers = Protocol::GRPC::Metadata.build(timeout: 5.0)
path = Protocol::GRPC::Route.build("hello.Greeter", "SayHello")

# Create HTTP request
request = Protocol::HTTP::Request["POST", path, headers, body]
```

### Reading a Response

Read gRPC responses using `Protocol::GRPC::Body::Readable`:

``` ruby
require "protocol/grpc/body/readable"

# Read response body
readable_body = Protocol::GRPC::Body::Readable.new(
	response.body,
	message_class: Hello::HelloReply
)

message = readable_body.read
readable_body.close

# Check gRPC status
status = Protocol::GRPC::Metadata.extract_status(response.headers)
if status != Protocol::GRPC::Status::OK
	message = Protocol::GRPC::Metadata.extract_message(response.headers)
	raise Protocol::GRPC::Error.for(status, message)
end

# Use canonical gRPC status names for logging, metrics, or serialization:
Protocol::GRPC::Status::NAMES[status] # => "OK"
```

### Limiting Message Size

To bound the size of messages received from a peer, pass `maximum_size` to the readable body:

``` ruby
readable_body = Protocol::GRPC::Body::Readable.new(
	response.body,
	message_class: Hello::HelloReply,
	maximum_size: 4 * 1024 * 1024
)

begin
	message = readable_body.read
ensure
	readable_body.close
end
```

The limit applies to each message's payload, excluding the five-byte frame prefix. Exactly the configured number of bytes is accepted. The declared frame length is checked before reading its payload, and gzip/deflate output is checked incrementally before it is accumulated. Exceeding either limit raises {ruby Protocol::GRPC::Error} with `RESOURCE_EXHAUSTED` status.

The underlying HTTP body may already have buffered a chunk containing multiple frames; this option bounds gRPC message processing, not transport buffering. The default is `nil` (unlimited). A limit of zero allows only empty, uncompressed payloads.

### Server Middleware

Create a server middleware by subclassing `Protocol::GRPC::Middleware`:

``` ruby
require "protocol/grpc/middleware"

class MyMiddleware < Protocol::GRPC::Middleware
	protected
	
	def dispatch(request)
		# Parse the service and method from the path:
		service_name, method_name = Protocol::GRPC::Route.parse(request.path)
		
		# Handle the request using service_name and method_name.
		# ...
	end
end
```

### Call Context

{ruby Protocol::GRPC::Call} provides context for a gRPC call:

``` ruby
require "protocol/grpc/call"

call = Protocol::GRPC::Call.new(request, response, deadline: deadline)

# Access request
call.request  # => Protocol::HTTP::Request

# Check deadline and status
call.deadline.exceeded?  # => false
call.status  # => Protocol::GRPC::Status::OK

# Access peer information
call.peer  # => Protocol::HTTP::Address
```
