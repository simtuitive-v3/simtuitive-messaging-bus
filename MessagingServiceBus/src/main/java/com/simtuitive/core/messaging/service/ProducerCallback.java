package com.simtuitive.core.messaging.service;

public interface ProducerCallback {
	
	/**
	 * Called when the message has been successfully published to the
	 * topic.
	 * 
	 * @param topic the topic name.
	 */
	public void onSuccess(String topic, String messagePayload);

	/**
	 * Called if an error occurred while trying to publish to the
	 * topic.
	 * 
	 * @param topic the topic name.
	 */
	public void onFailure(String topic,String messagePayload, Exception ex);
}
