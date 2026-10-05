# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2025-2026, by Samuel Williams.

require "protocol/http"
require "protocol/http/body/wrapper"
require "zlib"

require_relative "../error"
require_relative "../status"

module Protocol
	module GRPC
		# @namespace
		module Body
			# Represents a readable body for gRPC messages with length-prefixed framing.
			# This is the standard readable body for gRPC - all gRPC responses use message framing.
			# Wraps the underlying HTTP body and transforms raw chunks into decoded gRPC messages.
			class Readable < Protocol::HTTP::Body::Wrapper
				# Wrap the body of a message.
				#
				# @parameter message [Request | Response] The message to wrap.
				# @parameter options [Hash] The options to pass to the initializer.
				# @returns [Readable | Nil] The wrapped body or `nil` if the message has no body.
				def self.wrap(message, **options)
					if body = message.body
						message.body = self.new(body, **options)
					end
					
					return message.body
				end
				
				# Initialize a new readable body for gRPC messages.
				# @parameter body [Protocol::HTTP::Body::Readable] The underlying HTTP body
				# @parameter message_class [Class | Nil] Protobuf message class with .decode method.
				#   If `nil`, returns raw binary data (useful for channel adapters)
				# @parameter encoding [String | Nil] Compression encoding (from grpc-encoding header)
				# @parameter maximum_size [Integer | Nil] Maximum wire and decompressed payload size in bytes, excluding the frame prefix. `nil` disables the limit.
				# @raises [ArgumentError] If the maximum size is not a non-negative integer or `nil`.
				def initialize(body, message_class: nil, encoding: nil, maximum_size: nil)
					unless maximum_size.nil? || (maximum_size.is_a?(Integer) && maximum_size >= 0)
						raise ArgumentError, "Maximum size must be a non-negative integer or nil!"
					end
					
					super(body)
					@maximum_size = maximum_size
					@message_class = message_class
					@encoding = encoding
					@buffer = String.new.force_encoding(Encoding::BINARY)
				end
				
				# @attribute [String | Nil] The compression encoding.
				attr_reader :encoding
				
				# @attribute [Integer | Nil] The maximum wire and decompressed payload size in bytes.
				attr_reader :maximum_size
				
				# Read the next gRPC message.
				# Overrides Wrapper#read to transform raw HTTP body chunks into decoded gRPC messages.
				# Errors raised by the underlying body propagate unchanged.
				# @returns [Object | String | Nil] Decoded message, raw binary, or `Nil` if stream ended
				# @raises [Error] With `RESOURCE_EXHAUSTED` if a message exceeds the maximum size.
				def read
					# Read 5-byte prefix: 1 byte compression flag + 4 bytes length
					prefix = read_exactly(5)
					return nil unless prefix
					
					compressed = prefix[0].unpack1("C") == 1
					length = prefix[1..4].unpack1("N")
					check_size!(length)
					
					# Read the message body:
					data = read_exactly(length)
					unless data
						raise Error.new(Status::INTERNAL, "Truncated gRPC frame: expected #{length} bytes, received 0")
					end
					
					# Decompress if needed:
					data = decompress(data) if compressed
					
					# Decode using message class if provided, otherwise return binary:
					# This allows binary mode for channel adapters
					if @message_class
						# Use protobuf gem's decode method:
						@message_class.decode(data)
					else
						data # Return raw binary
					end
				end
				
			private
				
				# Read exactly n bytes from the underlying body.
				# @parameter n [Integer] The number of bytes to read.
				# @returns [String | Nil] The data read, or `Nil` if the stream ended before reading any bytes.
				# @raises [Error] If the stream ends after reading a partial value.
				def read_exactly(n)
					# Fill buffer until we have enough data:
					while @buffer.bytesize < n
						# An empty body can still have a pending error, so read to determine EOF:
						chunk = @body&.read
						
						if chunk.nil?
							return nil if @buffer.empty?
							
							raise Error.new(Status::INTERNAL, "Truncated gRPC frame: expected #{n} bytes, received #{@buffer.bytesize}")
						end
						
						# Append to buffer:
						@buffer << chunk.force_encoding(Encoding::BINARY)
					end
					
					# Extract the required data:
					data = @buffer[0...n]
					@buffer = @buffer[n..]
					data
				end
				
				# Decompress data using the configured encoding.
				# @parameter data [String] The compressed data.
				# @returns [String] The decompressed data.
				# @raises [Error] If decompression fails or the maximum size is exceeded.
				def decompress(data)
					case @encoding
					when "gzip"
						if @maximum_size
							inflate(data, Zlib::MAX_WBITS + 16)
						else
							Zlib.gunzip(data)
						end
					when "deflate"
						if @maximum_size
							inflate(data, Zlib::MAX_WBITS)
						else
							Zlib::Inflate.inflate(data)
						end
					else
						raise Error.new(Status::UNIMPLEMENTED, "Unsupported compression encoding: #{@encoding.inspect}")
					end
				rescue Zlib::Error => error
					raise Error.new(Status::INTERNAL, "Failed to decompress message: #{error.message}")
				end
				
				# Inflate a message without accumulating output beyond the maximum size.
				# @parameter data [String] The compressed data.
				# @parameter window_bits [Integer] The compression format.
				# @returns [String] The decompressed data.
				# @raises [Error] If the message is truncated or the maximum size is exceeded.
				# @raises [Zlib::Error] If decompression fails.
				def inflate(data, window_bits)
					inflater = Zlib::Inflate.new(window_bits)
					result = String.new.force_encoding(Encoding::BINARY)
					
					# Check each output chunk before retaining it:
					inflater.inflate(data) do |chunk|
						check_size!(result.bytesize + chunk.bytesize)
						result << chunk
					end
					
					unless inflater.finished?
						raise Error.new(Status::INTERNAL, "Truncated compressed gRPC message!")
					end
					
					result
				ensure
					inflater&.close
				end
				
				# Check a message size against the configured maximum.
				# @parameter size [Integer] The payload size in bytes.
				# @raises [Error] With `RESOURCE_EXHAUSTED` if the maximum size is exceeded.
				def check_size!(size)
					if @maximum_size && size > @maximum_size
						raise Error.new(Status::RESOURCE_EXHAUSTED, "Received message larger than maximum size: #{size} > #{@maximum_size}")
					end
				end
			end
		end
	end
end
