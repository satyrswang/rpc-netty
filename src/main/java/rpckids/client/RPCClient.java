package rpckids.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;

import rpckids.common.MessageDecoder;
import rpckids.common.MessageEncoder;
import rpckids.common.MessageOutput;
import rpckids.common.MessageRegistry;
import rpckids.common.RequestId;

//每个client有，bootstrap eventloopgroup channel

public class RPCClient {

	private final static Logger LOG = LoggerFactory.getLogger(RPCClient.class);

	private String ip;
	private int port;
	private Bootstrap bootstrap;
	private EventLoopGroup group;
	private MessageCollector collector; //channel
	private boolean started;
	private boolean stopped;
	private MessageRegistry registry = new MessageRegistry();

	public RPCClient(String ip, int port) {
		this.ip = ip;
		this.port = port;
		this.init();
	}

	public RPCClient rpc(String type, Class<?> reqClass) {
		registry.register(type, reqClass);
		return this;
	}

	public <T> RpcFuture<T> sendAsync(String type, Object payload) {
		if (!started) {
			connect();
			started = true;
		}
		String requestId = RequestId.next();
		//将id type object封装成对象使用send
		MessageOutput output = new MessageOutput(requestId, type, payload);
		return collector.send(output);
	}

	public <T> T send(String type, Object payload) {
		RpcFuture<T> future = sendAsync(type, payload);
		try {
			//由future来获得send的结果
			return future.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RPCException(e);
		}
	}

	//初始化的工作：包括Bootstrap和NioEventLoopGroup的创建和绑定、channel
	//channel需要知道是哪个client、且有哪些input（type 和 object）
	//bootstrap中创建socketchannel，添加handler--在socketchannel中创建pipe，对pipe加入channel 、encoder&decoder 和其他一些handler
	//对bootstrap进行配置
	public void init() {
		bootstrap = new Bootstrap();
		group = new NioEventLoopGroup(1);
		bootstrap.group(group);
		MessageEncoder encoder = new MessageEncoder();
		collector = new MessageCollector(registry, this);
		bootstrap.channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline pipe = ch.pipeline();
				pipe.addLast(new ReadTimeoutHandler(60));
				pipe.addLast(new MessageDecoder());//对response进行decoder
				pipe.addLast(encoder);
				pipe.addLast(collector);
			}

		});
		bootstrap.option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.SO_KEEPALIVE, true);
	}

	public void connect() {
		bootstrap.connect(ip, port).syncUninterruptibly();
	}

	public void reconnect() {
		if (stopped) {
			return;
		}
		bootstrap.connect(ip, port).addListener(future -> {
			if (future.isSuccess()) {
				return;
			}
			if (!stopped) {//eventloopgroup为连接服务进行重连 -- 递归重连

				group.schedule(() -> {
					reconnect();
				}, 1, TimeUnit.SECONDS);
			}
			LOG.error("connect {}:{} failure", ip, port, future.cause());
		});
	}

	public void close() {
		stopped = true;
		//close要关掉channel和eventloopgroup
		collector.close();
		group.shutdownGracefully(0, 5000, TimeUnit.SECONDS);
	}

}
