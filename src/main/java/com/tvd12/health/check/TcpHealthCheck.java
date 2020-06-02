package com.tvd12.health.check;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpHealthCheck implements HealthCheck {

	protected final int listenPort;
	protected final String loopbackIp;
	protected final int connectionMaxIdleTime;
	protected ByteBuffer buffer;
	protected Selector acceptSelector;
	protected Selector readSelector;
	protected volatile boolean active;
	private ServerSocket serverSocket;
	private ServerSocketChannel serverSocketChannel;
	protected List<SocketChannel> idleChannels;
	protected Map<SocketChannel, Long> aliveChannels;
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	public TcpHealthCheck() {
		this(3000);
	}
	
	public TcpHealthCheck(int connectionMaxIdleTime) {
		this(9000, connectionMaxIdleTime);
	}
	
	public TcpHealthCheck(int listenPort, int connectionMaxIdleTime) {
		this("0.0.0.0", listenPort, connectionMaxIdleTime);
	}
	
	public TcpHealthCheck(
			String loopbackIp, 
			int listenPort, int connectionMaxIdleTime) {
		this.loopbackIp = loopbackIp;
		this.listenPort = listenPort;
		this.connectionMaxIdleTime = connectionMaxIdleTime;
	}
	
	@Override
	public void start() throws Exception {
		if(active)
			throw new IllegalStateException("tcp health check has started");
		this.active = true;
		this.idleChannels = new ArrayList<>();
		this.buffer = ByteBuffer.allocateDirect(1024);
		this.acceptSelector = Selector.open();
		this.readSelector = Selector.open();
		this.aliveChannels = new ConcurrentHashMap<>();
		this.serverSocketChannel = ServerSocketChannel.open();
		this.serverSocketChannel.configureBlocking(false);
		this.serverSocket = serverSocketChannel.socket();
		this.serverSocket.setReuseAddress(true);
		this.serverSocket.bind(new InetSocketAddress(loopbackIp, listenPort));
		this.serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
		this.doStart();
	}
	
	protected void doStart() {
		Thread thread = new Thread(() -> loop());
		thread.setName("tcp-health-check");
		thread.start();
	}
	
	protected void loop() {
		while(active) {
			try {
				Thread.sleep(2);
				handleLoopOne();
			}
			catch (Exception e) {
				logger.warn("handle health check error", e);
			}
		}
	}
	
	protected void handleLoopOne() throws Exception {
		removeIdleConnections();
		acceptionConnections();
		readSocketBytes();
	}
	
	protected void removeIdleConnections() {
		Long currentTime = System.currentTimeMillis();
		for(SocketChannel channel : aliveChannels.keySet()) {
			Long lastRequestTime = aliveChannels.get(channel);
			Long offset = currentTime - lastRequestTime;
			if(offset >= connectionMaxIdleTime)
				idleChannels.add(channel);
		}
		for(SocketChannel channel : idleChannels)
			aliveChannels.remove(channel);
		idleChannels.clear();
	}
	
	protected void acceptionConnections() throws Exception {
		int readyKeyCount = acceptSelector.select(3);
		if(readyKeyCount <= 0)
			return;
		Set<SelectionKey> readyKeys = acceptSelector.selectedKeys();
		Iterator<SelectionKey> iterator = readyKeys.iterator();
		while(iterator.hasNext()) {
			SelectionKey key = iterator.next();
			iterator.remove();
			acceptReadyKey(key);
		}
		readSelector.wakeup();
	}
	
	protected void acceptReadyKey(SelectionKey key) throws Exception {
		if(key.isAcceptable()) {
			ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
			SocketChannel clientChannel = serverChannel.accept();
			clientChannel.configureBlocking(false);
			clientChannel.socket().setTcpNoDelay(false);
			clientChannel.register(readSelector, SelectionKey.OP_READ);
			aliveChannels.put(clientChannel, System.currentTimeMillis());
		}
	}
	
	protected void readSocketBytes() throws Exception {
		int readyKeyCount = readSelector.selectNow();
		if(readyKeyCount <= 0)
			return;
		Set<SelectionKey> readyKeys = this.readSelector.selectedKeys();
		Iterator<SelectionKey> iterator = readyKeys.iterator();
		while(iterator.hasNext()) {
			SelectionKey key = iterator.next();
			iterator.remove();
			if(key.isValid()) {
				processReadyKey(key);
			}
		}
	}
	
	protected void processReadyKey(SelectionKey key) throws Exception {
		if(key.isWritable()) {
			key.interestOps(SelectionKey.OP_READ);
		}
		if(key.isReadable()) {
			processReadableKey(key);
		}
	}
	
	protected void processReadableKey(SelectionKey key) throws Exception {
		int readBytes = -1;
		Exception exception = null;
		SocketChannel channel = (SocketChannel) key.channel();
		try {
			buffer.clear();
			readBytes = channel.read(buffer);
		}
		catch (Exception e) {
			exception = e;
		}
		if(readBytes == -1) {
			channel.close();
			aliveChannels.remove(channel);
		}
		else if(readBytes > 0) {
			processReadBytes(channel);
			aliveChannels.put(channel, System.currentTimeMillis());
		}
		if(exception != null)
			logger.info("I/O error at socket-reader: {}({})", exception.getClass().getName(), exception.getMessage());
	}
	
	protected void processReadBytes(SocketChannel channel) throws Exception {
		buffer.flip();
		channel.write(buffer);
	}
	
	@Override
	public void stop() {
		this.active = false;
		if(serverSocket != null)
			closeComponent("server socket", serverSocket);
		if(serverSocketChannel != null)
			closeComponent("server socket channel", serverSocketChannel);
		if(acceptSelector != null)
			closeComponent("accept selector", acceptSelector);
		if(readSelector != null)
			closeComponent("read selector", readSelector);
		this.serverSocket = null;
		this.acceptSelector = null;
		this.readSelector = null;
		this.serverSocketChannel = null;
	}
	
	protected void closeComponent(String selectorName, Closeable component) {
		try {
			component.close();
		}
		catch (Exception e) {
			logger.error("close: " + selectorName + " error", e);
		}
	}
	
	public int getAliveConnectionCount() {
		return aliveChannels.size();
	}
	
}
