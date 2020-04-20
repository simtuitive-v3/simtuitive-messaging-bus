package com.simtuitive.core.messaging.service;


/**
 *
 * {@link IConsumerCallback} interface for the callback of consumer
 *
 */
public interface IConsumerCallback {
  public boolean message(String topicName, String message);
}
