package com.simtuitive.core.messaging.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simtuitive.core.messaging.util.MessagingHelper;


/**
 *
 * The {@link MessagingService} class for the implementation of the {@link IMessagingService} interface to do the
 * authorization, produce and consume of the messages
 *
 */
public class MessagingService implements IMessagingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessagingService.class);
  private static final String CALLBACK_NAME = "MessageProcessorCallBack";

  private static MessagingService messagingService;
   
  /**
   * The private contructor to implement the Singleton
   */
  private MessagingService() {
  }

  /**
   * This method is designed to get the messaging service instance.\
   * It is a singletonFactory Method.
   * @param bootStrapServer this input parameter signifies the brokerHostIP:Port
   * @param serviceName this input parameter signifies the current service name 
   * @param topicCallBackMap this input parameter signifies the {Topic-->TopicCallBack} map
   * @param connectionStateCallBack this input parameter signifies the callback to handle 
   * 		most of the failure scenarios. 
   * @return MessagingService This returns a reference to the messaging service.
 * @throws Exception 
   */
  public static synchronized MessagingService getMessagingService(String bootStrapServer,String serviceName,
		  														  Map<String,IConsumerCallback> topicCallBackMap,
		  														  Properties userConsumerProperties,
		  														  Properties userProducerProperties,
		  														  IConsumerStateCallback connectionStateCallBack) throws Exception {
    LOGGER.debug("MessagingService - getMessagingService() - start");
    if (messagingService == null) {
        messagingService = new MessagingService();
        
    MessagingHelper.configKafaProperties(bootStrapServer,serviceName,userConsumerProperties,userProducerProperties);
         
     /**
      * Subscribe to each eligible topics... 
      * 1. Fetch/Extract all the eligible topics for this service, from the map,
      * 3. Construct the list of eligible topics. 
      * 4. Each TOPIC will have a consumer to process the arrived messages.
      * 5. Each Topic's processor is going to be designed as implementation class for 
      *    "IConsumerCallback". 
      * 6. Specification of the name of the call back --> TopicName + "MessageProcessorCallBack"
      * 7. Ex : If Topic Name is {FulfilmentOps} the the call back --> "FulfilmentOpsMessageProcessorCallBack"
      * 8. Each service will configure a property in the service's {application.properties} file as 
      *    consumer.callback.package.path = com.simtuitive.core.messaging.service.callback
      * 9. {consumerCallBackPackagePath}, this is the new parameter injected into this method invocation.
      * 10.All the required call backs will be dynamically loaded and instantiated by the messaging service.
      */
     subscribeToEachEligibleTopics(topicCallBackMap,connectionStateCallBack);
     LOGGER.debug("Successfully subscribed to all topics...");
    }
    //Finally Return the reference.
    return messagingService;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.innominds.messaging.service.IProducerService#publishMessage(java.lang.String, org.json.JSONObject)
   */
  @Override
  public void publishMessage(String topicName, String messageStr,ProducerCallback topicProducerCallBack) throws Exception {
	LOGGER.debug("MessagingService - publishMessage() - start");
	if(StringUtils.isNotBlank(topicName) && StringUtils.isNotBlank(messageStr)){
	   MessagingHelper.runProducer(topicName, messageStr,topicProducerCallBack);
	}else{
		LOGGER.debug("Invalid parameters.");
	}
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.simtuitive.core.messaging.service.IProducerService#publishMessage(java.util.List, org.json.JSONObject)
   */
  @Override
  public void publishMessage(List<String> topicNames, String messageStr, Map<String, ProducerCallback> topicProducerCallBackMap)
      throws Exception {
	  LOGGER.debug("MessagingService - publishMessage() - start");
    
    try {
          for (String topicName : topicNames) {
        	  if(topicProducerCallBackMap != null)
        	     MessagingHelper.runProducer(topicName, messageStr,topicProducerCallBackMap.get(topicName));
        	  else
        		 MessagingHelper.runProducer(topicName, messageStr, null); 
          }
    }catch(Exception e){
        	LOGGER.error("Exception in MessagingService - publishMessage() : " , e);
            throw e;
     }
   }
  
	/**
	 * Facade, method is designed to handle all the steps 
	 * of execution for subscribing each eligible topics.
	 * @param allExistingTopicData
	 * @param necessaryCallBacks
	 * @throws JSONException
	 * @throws Exception
	 */
	public static void subscribeToEachEligibleTopics(Map<String,IConsumerCallback> topicCallBackMap,
													 IConsumerStateCallback connectionStateCallBack) throws Exception{
		//Fetch all the eligible topics from AdminAPI, required for subscribing to. 
		List<String> topics = listOfEligibleTopicsToConsumeMessages(topicCallBackMap);
		for(String topic : topics){
			/**
			 * Each Topic should have a managed consumer, to process the message without
			 * collision.Each Topic will have a specific type (Pay-load, the content)
			 * publisher sent, to solve use case purpose. 
			 * Hence, a specific Call Back is necessary for each topic. 
			 * 1. Configure package structure in the property file application.properties (Service-Side)
			 * 2. Name Of the call back --> topicName + "MessageProcessorCallBack"
			 * 3. At this point, package name, topic name and the constant part is known
			 * 4. Number of call backs = Number of Eligible Topics
			 * 5. Construct the fully qualified class name.
			 * 6. Load the class name dynamically.
			 * 7. Instantiate the class, return {IConsumerCallback} 
			 */
			//IConsumerCallback requiredCallBack = getInstanceOfCallBackForTopic(topic,consumerCallBackPackagePath);
			//Fetch from the injected Map
			IConsumerCallback requiredCallBack = topicCallBackMap.get(topic);
			
			if(topicCallBackMap != null){
				if(StringUtils.isNotBlank(topic)){
					if(requiredCallBack != null){
						//Now, proceed further to subscribe to the topic.
						MessagingHelper.subcribeToTopic(topic, requiredCallBack,connectionStateCallBack);
					}else{
						//If callback is NULL, log the appropriate message and skip the topic.
						LOGGER.debug("Topic : " + topic + " is not associated with any call back.");
						LOGGER.debug("Topic : " + topic + " is not elligible to subcribe.");
					}
				}else{
					//If the topic name is NULL or Blank, log appropriate message and skip this invalid topic
					LOGGER.debug("Topic Name is NULL or blank, so this entry has been ignored. ");
				}
			}
			
			LOGGER.debug("Eligible topics have been subcribed successfully...");
		}
	}
	
	/**
	 * Collect all the consumable topics, for the current service.
	 * @param topicCallBackMap
	 * @return List<String> returns all eligible consumable topic list
	 * @throws JSONException
	 */
	public static List<String> listOfEligibleTopicsToConsumeMessages(Map<String,IConsumerCallback> topicCallBackMap){
		LOGGER.debug("The processing for collection of consumable topics have been started...");
		
		List<String> listOfConsumableTopics = new ArrayList<>();
		if(topicCallBackMap != null)
			listOfConsumableTopics.addAll(topicCallBackMap.keySet());
				
		return listOfConsumableTopics;
	}
	
	/**
	 * This logic has been designed to, isolate all the service from the 
	 * appropriate class back instantiation.
	 * Name of the callBack, constructed dynamically.By taking up following
	 * parameters into consideration.
	 * 1. consumerCallBackPackagePath
	 * 2. topicname
	 * 3. 
	 * @param topicName
	 * @param consumerCallBackPackagePath
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static IConsumerCallback getInstanceOfCallBackForTopic(String topicName,
																  String consumerCallBackPackagePath) throws IllegalAccessException,
																											 InstantiationException{
		//Construct fully qualified appropriate callback class name
		String fullyQualifiedClassName = consumerCallBackPackagePath + "." + topicName + CALLBACK_NAME;
		LOGGER.debug("Call Back for this topic is : " + fullyQualifiedClassName);
		Class callBackClOb = null;
		IConsumerCallback retObj = null;
		try{
			//Load the appropriate class dynamically...
			callBackClOb =  Class.forName(fullyQualifiedClassName);
			LOGGER.debug("Call Back for this topic : " + fullyQualifiedClassName + " is successfully loaded.");
		}catch(ClassNotFoundException cne){
			//Log the message and proceed further
			LOGGER.debug("Call Back implementation class is not available for this service : ");
		}
		if(callBackClOb != null){
			//Instantiate the loaded class dynamically...
			retObj = (IConsumerCallback)callBackClOb.newInstance();
			LOGGER.debug("Call Back for this topic : " + fullyQualifiedClassName + " is successfully instantiated.");
		}
		
		return retObj;
	}
}