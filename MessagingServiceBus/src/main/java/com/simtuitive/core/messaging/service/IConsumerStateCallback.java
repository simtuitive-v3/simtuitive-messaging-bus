package com.simtuitive.core.messaging.service;

public interface IConsumerStateCallback {
	/**
	 * This is going to capture most of the failure scenarios
	 * related to all topics.
	 * @param topicName This parameter can be {NULL}
	 * @param ex
	 */
	 public void onConsumerStateFailure(String topicName, Exception ex);
}
