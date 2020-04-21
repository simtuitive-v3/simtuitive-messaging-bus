package com.simtuitive.core.messaging.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simtuitive.core.messaging.service.IConsumerStateCallback;

public class ConsumerStateFailureCallback implements IConsumerStateCallback{
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerStateFailureCallback.class);
	public void onConsumerStateFailure(String topicName, Exception ex){
		LOGGER.info("ConsumerStateFailureCallback, consumer is not in a appropriate state for the - topic : " , topicName);
	    System.out.println("Consumer for this topic is in broken state ########  " + topicName);
	    /**
	     * Process the exception appropriately, in the client...
	     */
	    LOGGER.info("Save The Message Payload For Furture Processing...");
	}

}
