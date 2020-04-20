package com.simtuitive.core.messaging.service;


import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.simtuitive.core.messaging.enumeration.EResponseCode;
import com.simtuitive.core.messaging.service.IConsumerCallback;
import com.simtuitive.core.messaging.service.IConsumerStateCallback;
import com.simtuitive.core.messaging.service.IMessagingService;
import com.simtuitive.core.messaging.service.MessagingService;

public class MessagingServicesTest {
  // Instantiating singletons for using Kafka SDK.
  static IMessagingService iMsgService = null;
  static Map<String,IConsumerCallback> topicCallBackMap = new HashMap<String, IConsumerCallback>();
  static IConsumerStateCallback conStateCallback = new ConsumerStateFailureCallback();
  static{
	  FulfilmentOpsMessageProcessorCallBack fulObjCallBack = new FulfilmentOpsMessageProcessorCallBack();
	  topicCallBackMap.put("FulfilmentOps",fulObjCallBack);
}

  @BeforeClass
  public static void initMsgService() throws Exception {
	 iMsgService = MessagingService.getMessagingService("localhost:9092","orderService",topicCallBackMap,null,null,conStateCallback);
  }

  @Before
  public void beforeEachTest() {
    System.out.println("This is executed before each Test");
  }

  @After
  public void afterEachTest() {
    System.out.println("This is exceuted after each Test");
  }

  @Ignore
  @Test
  public void testAuthorize() {
    try{
         iMsgService = MessagingService.getMessagingService("localhost:9092","orderService",topicCallBackMap,null,null,conStateCallback);
    }catch (Exception e) {
      e.printStackTrace(System.err);
    }
  }

  @Ignore
  @Test
  public void testPublishMessage() throws Exception {

    try{

        iMsgService.publishMessage("KafkaTopic", "Tech M Bangalore".toString(),null);
    }catch(Exception e){
      e.printStackTrace(System.err);
    }
  }

  @Ignore
  @Test
  public void testEqual() throws Exception {

    try{
         iMsgService.publishMessage("KafkaTopic", "Tech M Bangalore",null);
    }catch(Exception e) {
      e.printStackTrace(System.err);
    }
  }

}