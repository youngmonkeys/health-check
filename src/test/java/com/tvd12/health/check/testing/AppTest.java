package com.tvd12.health.check.testing;

import com.tvd12.health.check.TcpHealthCheck;

public class AppTest {

	public static void main(String[] args) throws Exception {
		TcpHealthCheck check = new TcpHealthCheck(3000);
		check.start();
		while(true) {
			Thread.sleep(1000);
			System.out.println("size: " + check.getAliveConnectionCount());
		}
	}
	
}
