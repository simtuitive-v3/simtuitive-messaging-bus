package com.simtuitive.core.messaging.service;

import java.util.HashMap;
import java.util.Map;

import com.simtuitive.core.messaging.service.IConsumerCallback;
import com.simtuitive.core.messaging.service.IConsumerStateCallback;
import com.simtuitive.core.messaging.service.MessagingService;

/**
 *
 * Class details
 *
 */
public class FulfilmentOpsTopicTest {
  public static void main(String[] args) {
    String msg1 = "{\"message\": \"order is created\"}";
    String msg2 = "{\"message\": \"shipping is created\"}";
    String msg3 = "{\"message\": \"payment is created\"}";
        
    IConsumerStateCallback conStateCallback = new ConsumerStateFailureCallback();
    Map<String,IConsumerCallback> topicCallBackMap = new HashMap<String, IConsumerCallback>();
    FulfilmentOpsMessageProcessorCallBack fulObjCallBack = new FulfilmentOpsMessageProcessorCallBack();
	topicCallBackMap.put("FulfilmentOps",fulObjCallBack);
	
    try {
      MessagingService messagingService = MessagingService.getMessagingService("localhost:9092","orderService",topicCallBackMap,null,null,conStateCallback);
      messagingService.publishMessage("FulfilmentOps", msg2,null);
      MessagingService.getMessagingService("localhost:9092","orderService",topicCallBackMap,null,null,conStateCallback);
      messagingService.publishMessage("FulfilmentOps", msg3,null);
      MessagingService.getMessagingService("localhost:9092","orderService",topicCallBackMap,null,null,conStateCallback);
    
      String callBackString = FulfilmentOpsMessageProcessorCallBack.messageObject;
      System.out.println(callBackString);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
	/*public static void main(String[] args) {
		if (Configuration.get().getKafkaConsumers() != null && Configuration.get().getKafkaConsumers().size() > 0) {
            ExecutorService executor = Executors.newFixedThreadPool(Configuration.get().getKafkaConsumers().size() * 2);
            for (Configuration.KafkaConsumer consumer : Configuration.get().getKafkaConsumers()) {
                executor.submit(new KafkaConsumerRunner(null));
                if (consumer.failureTopic != null) {
                	Properties prop = new Properties();
                    executor.submit(new KafkaConsumerFailedRunner(prop));
                }
            }
        }
	}*/
	
}
