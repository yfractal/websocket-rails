require "redis"
require "redis/connection/ruby"
require "connection_pool"
require "redis-objects"

module WebsocketRails

  module Synchronization

    def self.sync
      @sync ||= Synchronize.new
    end

    class Synchronize

      delegate :connection_manager, to: WebsocketRails
      delegate :dispatcher, to: :connection_manager

      include Logging

      def initialize
        Redis::Objects.redis = redis
        @dispatch_queue = EventQueue.new
        @publish_queue = EventQueue.new
        @channel_tokens = Redis::HashKey.new('websocket_rails.channel_tokens')
        @active_servers = Redis::List.new('websocket_rails.server_tokens')
        @active_users = Redis::HashKey.new('websocket_rails.users', marshal: true)
      end

      def redis_pool(redis_options)
        ConnectionPool::Wrapper.new(size: WebsocketRails.config.synchronize_pool_size) do
          Redis.new(redis_options)
        end
      end

      def redis
        @redis ||= begin
          redis_options = WebsocketRails.config.redis_options
          EM.reactor_running? ? redis_pool(redis_options) : ruby_redis
        end
      end

      def ruby_redis
        @ruby_redis ||= begin
          redis_options = WebsocketRails.config.redis_options.merge(:driver => :ruby)
          redis_pool(redis_options)
        end
      end

      def publish_remote(message)
        message.server_token = server_token
        redis.publish "websocket_rails.events", message.serialize
      end

      def server_token
        @server_token
      end

      def synchronize!
        unless @synchronizing
          @publish_queue.pop do |message|
            publish_remote(message)
          end
          @dispatch_queue.pop do |message|
            process_inbound message
          end
          @server_token = generate_server_token
          register_server(@server_token)

          @synchro = Thread.new do
            fiber_redis = Redis.connect(WebsocketRails.config.redis_options)
            fiber_redis.subscribe "websocket_rails.events" do |on|

              on.message do |_, encoded_message|
                message = Event.deserialize(encoded_message, nil)

                # Do nothing if this is the server that sent this event.
                next if message.server_token == server_token

                # Ensure an event never gets triggered twice. Events added to the
                # redis queue from other processes may not have a server token
                # attached.
                message.server_token = server_token if message.server_token.nil?

                @dispatch_queue << message
              end
            end

            info "Beginning Synchronization"
          end

          @synchronizing = true

          trap('TERM') do
            Thread.new { shutdown!; exit }
          end
          trap('INT') do
            Thread.new { shutdown!; exit }
          end
          trap('QUIT') do
            Thread.new { shutdown!; exit }
          end
        end
      end

      def process_inbound(message)
        dispatcher.dispatch message
      end

      def shutdown!
        remove_server(server_token)
      end

      def generate_server_token
        begin
          token = SecureRandom.urlsafe_base64
        end while @active_servers.include? token

        token
      end

      def register_server(token)
        @active_servers << token
        info "Server Registered: #{token}"
      end

      def remove_server(token)
        @active_servers.delete token
        info "Server Removed: #{token}"
      end

      def register_remote_user(connection)
        id = connection.user_identifier
        user = connection.user
        @active_users[id] = user
      end

      def destroy_remote_user(identifier)
        @active_users.delete identifier
      end

      def find_remote_user(identifier)
        @active_users[identifier]
      end

      def all_remote_users
        @active_users.all
      end

      def channel_tokens
        @channel_tokens
      end

      def register_channel(name, token)
        @channel_tokens[name] = token
      end
    end
  end
end
