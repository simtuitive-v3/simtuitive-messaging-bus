package com.simtuitive.core.messaging.service;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;

import com.simtuitive.core.messaging.enumeration.EResponseCode;

/**
 *
 * The {@link IMessagingService} interface for the operation of produce and consume of the messages
 *
 */
public interface IMessagingService {
  // This interface for publishing a message/event on the selected kafka topic
  public void publishMessage(String topicName, String messageObject,ProducerCallback topicProducerCallBack) throws Exception;

  // This interface for duplicating the message on set of kafka topics
  public void publishMessage(List<String> topicNames, String message,Map<String, ProducerCallback> topicProducerCallBackMap) throws Exception;

}
