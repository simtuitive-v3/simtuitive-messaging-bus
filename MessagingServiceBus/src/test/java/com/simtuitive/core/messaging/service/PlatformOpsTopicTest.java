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
public class PlatformOpsTopicTest {
  
  public static void main(String[] args) {
    String msg = "{\"message\": \"Invite to client is ready to sent\"}";
    /**
     *  If consumer state fails dynamically, this call back will take care of the payload.
     *  {PlatformOps} The Named Topic.
     */
    IConsumerStateCallback conStateCallback = new ConsumerStateFailureCallback();
    Map<String,IConsumerCallback> topicCallBackMap = new HashMap<String, IConsumerCallback>();
    //Processing Unit
    FulfilmentOpsMessageProcessorCallBack fulObjCallBack = new FulfilmentOpsMessageProcessorCallBack();
    topicCallBackMap.put("FulfilmentOps",fulObjCallBack);
    try{
         MessagingService messagingService = MessagingService.getMessagingService("localhost:9092","orderService",topicCallBackMap,null,null,conStateCallback);
         messagingService.publishMessage("PlatformOps", msg,null);
     
      String callBackString = FulfilmentOpsMessageProcessorCallBack.messageObject;
      System.out.println(callBackString);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
