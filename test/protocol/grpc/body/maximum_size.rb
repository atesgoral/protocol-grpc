# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2026, by Ateş Göral.

require "protocol/grpc/body/readable"
require "protocol/http/body/buffered"
require "protocol/http/body/writable"
require_relative "../../../../fixtures/protocol/grpc/test_message"

require "zlib"

describe Protocol::GRPC::Body::Readable do
	with "maximum size" do
		let(:source_body) {Protocol::HTTP::Body::Buffered.new}
		let(:body) {subject.new(source_body, maximum_size: 1024)}
		
		def frame(data, compressed: false)
			[compressed ? 1 : 0, data.bytesize].pack("CN") + data
		end
		
		def expect_size_error
			expect{yield}.to raise_exception(Protocol::GRPC::Error) do |error|
				expect(error.status_code).to be == Protocol::GRPC::Status::RESOURCE_EXHAUSTED
				expect(error.message).to be =~ /Received message larger than maximum size: \d+ > 1024/
			end
		end
		
		it "defaults to unlimited messages" do
			data = "a" * 2048
			source_body.write(frame(data))
			reader = subject.new(source_body)
			
			expect(reader.maximum_size).to be_nil
			expect(reader.read).to be == data
		end
		
		[-1, 1.5, "1024", false].each do |maximum_size|
			it "rejects an invalid maximum size #{maximum_size.inspect}" do
				expect{subject.new(source_body, maximum_size: maximum_size)}.to raise_exception(ArgumentError)
			end
		end
		
		it "accepts exactly the maximum payload size, excluding the prefix" do
			data = "a" * 1024
			source_body.write(frame(data))
			
			expect(body.maximum_size).to be == 1024
			expect(body.read).to be == data
		end
		
		it "accepts empty messages with a zero limit" do
			source_body.write(frame(""))
			reader = subject.new(source_body, maximum_size: 0)
			
			expect(reader.read).to be == ""
			expect(reader.read).to be_nil
		end
		
		it "rejects non-empty messages with a zero limit" do
			source_body.write(frame("a"))
			reader = subject.new(source_body, maximum_size: 0)
			
			expect{reader.read}.to raise_exception(Protocol::GRPC::Error) do |error|
				expect(error.status_code).to be == Protocol::GRPC::Status::RESOURCE_EXHAUSTED
			end
		end
		
		it "rejects an oversized declared payload without reading it" do
			source_body.write([0, 1025].pack("CN"))
			reads = 0
			mock(source_body) do |wrapper|
				wrapper.wrap(:read) do |original|
					reads += 1
					raise "Payload must not be read!" if reads > 1
					original.call
				end
			end
			
			expect_size_error {body.read}
			expect(reads).to be == 1
		end
		
		it "reads fragmented prefixes and payloads" do
			data = "a" * 1024
			frame(data).bytes.each_slice(3) do |bytes|
				source_body.write(bytes.pack("C*"))
			end
			
			expect(body.read).to be == data
			expect(body.read).to be_nil
		end
		
		it "limits each coalesced message independently" do
			data = "a" * 1024
			source_body.write(frame(data) + frame(data))
			
			expect(body.read).to be == data
			expect(source_body).to be(:empty?)
			expect(body.read).to be == data
			expect(body.read).to be_nil
		end
		
		it "rejects an oversized message after a complete coalesced message" do
			source_body.write(frame("a") + frame("b" * 1025))
			
			expect(body.read).to be == "a"
			expect_size_error {body.read}
		end
		
		it "preserves protobuf decoding within the limit" do
			message_class = Protocol::GRPC::Fixtures::TestMessage
			message = message_class.new(value: "Hello")
			source_body.write(frame(message.to_proto))
			reader = subject.new(source_body, message_class: message_class, maximum_size: message.to_proto.bytesize)
			
			expect(reader.read).to be == message
		end
		
		["\x00\x00".b, [0, 2].pack("CN"), [0, 2].pack("CN") + "a"].each do |partial_frame|
			it "rejects a truncated #{partial_frame.bytesize}-byte frame" do
				source_body.write(partial_frame)
				
				expect{body.read}.to raise_exception(Protocol::GRPC::Error) do |error|
					expect(error.status_code).to be == Protocol::GRPC::Status::INTERNAL
				end
			end
		end
		
		it "rejects an incomplete next frame after a complete message" do
			source_body.write(frame("a") + "\x00\x00".b)
			
			expect(body.read).to be == "a"
			expect{body.read}.to raise_exception(Protocol::GRPC::Error) do |error|
				expect(error.status_code).to be == Protocol::GRPC::Status::INTERNAL
			end
		end
		
		it "preserves a pending underlying read error" do
			source = Protocol::HTTP::Body::Writable.new
			error = IOError.new("Stream failed!")
			source.close_write(error)
			reader = subject.new(source, maximum_size: 1024)
			
			expect{reader.read}.to raise_exception(IOError).and(be_equal(error))
		end
		
		{"gzip" => Zlib.method(:gzip), "deflate" => Zlib::Deflate.method(:deflate)}.each do |encoding, compress|
			with encoding do
				let(:body) {subject.new(source_body, encoding: encoding, maximum_size: 1024)}
				
				it "accepts exactly the maximum decompressed size" do
					data = "a" * 1024
					source_body.write(frame(compress.call(data), compressed: true))
					
					expect(body.read).to be == data
				end
				
				it "rejects expansion beyond the limit before accumulating the full output" do
					data = "a" * 1_000_000
					compressed = compress.call(data)
					expect(compressed.bytesize).to be < 1024
					source_body.write(frame(compressed, compressed: true))
					sizes = []
					mock(body) do |wrapper|
						wrapper.wrap(:check_size!) do |original, size|
							sizes << size
							original.call(size)
						end
					end
					
					expect_size_error {body.read}
					expect(sizes.last).to be > 1024
					expect(sizes.last).to be < data.bytesize
				end
				
				it "resets the expanded-size limit for each message" do
					data = "a" * 1024
					message = frame(compress.call(data), compressed: true)
					source_body.write(message + message)
					
					expect(body.read).to be == data
					expect(body.read).to be == data
				end
				
				it "rejects a truncated compressed stream" do
					compressed = compress.call("a" * 512).byteslice(0...-1)
					source_body.write(frame(compressed, compressed: true))
					
					expect{body.read}.to raise_exception(Protocol::GRPC::Error) do |error|
						expect(error.status_code).to be == Protocol::GRPC::Status::INTERNAL
					end
				end
				
				it "rejects invalid compressed data" do
					source_body.write(frame("invalid", compressed: true))
					
					expect{body.read}.to raise_exception(Protocol::GRPC::Error) do |error|
						expect(error.status_code).to be == Protocol::GRPC::Status::INTERNAL
					end
				end
			end
		end
		
		it "preserves unsupported compression errors" do
			source_body.write(frame("data", compressed: true))
			reader = subject.new(source_body, encoding: "custom", maximum_size: 1024)
			
			expect{reader.read}.to raise_exception(Protocol::GRPC::Error) do |error|
				expect(error.status_code).to be == Protocol::GRPC::Status::UNIMPLEMENTED
			end
		end
	end
end
