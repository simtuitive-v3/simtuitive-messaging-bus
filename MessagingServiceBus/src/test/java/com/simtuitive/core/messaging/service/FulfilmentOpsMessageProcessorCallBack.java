package com.simtuitive.core.messaging.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simtuitive.core.messaging.service.IConsumerCallback;

public class FulfilmentOpsMessageProcessorCallBack implements IConsumerCallback {
	private static final Logger LOGGER = LoggerFactory.getLogger(FulfilmentOpsMessageProcessorCallBack.class);
	public static String messageObject;
	
	 @Override
	  public boolean message(String topicName, String messagePayload) {
	    LOGGER.info("FulfilmentOpsMessageProcessorCallBack - topic : " , topicName);
	    LOGGER.info("Consumed Message : " + messagePayload);
	    messageObject = messagePayload;
	    System.out.println("Message Arrived in FulfilmentOpsMessageProcessorCallBack ########  " + messagePayload);
	    return true;
	  }
}
