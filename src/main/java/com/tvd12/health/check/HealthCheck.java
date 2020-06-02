package com.tvd12.health.check;

public interface HealthCheck {

	void start() throws Exception;
	
	void stop();
	
}
