package com.simtuitive.core.messaging.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simtuitive.core.messaging.constants.MessagingConstants;
import com.simtuitive.core.messaging.service.IConsumerCallback;
import com.simtuitive.core.messaging.service.IConsumerStateCallback;
import com.simtuitive.core.messaging.service.ProducerCallback;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

/**
 *
 * The {@link MessagingHelper} class for doing all the utility operations
 *
 */
public class MessagingHelper {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(MessagingHelper.class);
  private static MessagingProperties messagingProperties = null;
  private static Properties consumerProperties = null;
  private static Properties producerProperties = null;
  private static Properties commonProperties = null;
  private final static AtomicBoolean closed = new AtomicBoolean(false);
  private static KafkaProducer<String, String> kafkaProducer;
  private static ExecutorService consumerThreadPool; 
  
  private static String serviceName;
  /**
   * counter for total consumers
   */
  
  private static Counter consumersCounter = 
  	Counter.build()
		   .name("innominds_msglib_consumers_total")
		   .labelNames("service")
		   .help("Total consumers.")
		   .register();
  
  /**
   * Counter for total published messages by status (success or failure) per topic.
   */
	private static Counter publishedMessagesCounter = 
		Counter.build()
		    .name("innominds_msglib_published_messages_total")
		    .labelNames("service","topic","status")
		    .help("Total messages published.")
		    .register();
	
	/**
	 * Counter for total messages failed to be consumed per topic.
	 */
  private static Counter failedMessagesCounter = 
  	Counter.build()
		    .name("innominds_msglib_messages_failed_to_consume_total")
		    .help("Total messages failed to consume.")
		    .labelNames("service","topic")
		    .register();
  
	/**
	 * Counter for total messages received which were ignored because of failure in an earlier message processing
	 * (per topic).
	 */
	private static Counter ignoredMessagesCounter = 
		Counter.build()
			.name("innominds_msglib_ignored_msgs_total")
			.help("Total ignored messages after failure.")
			.labelNames("service","topic")
			.register();

	/**
	 * Counter for total number of failed commits per topic.
	 */
	private static Counter failedCommitsCounter = 
		Counter.build()
			.name("innominds_msglib_failed_commits_total")
			.help("Total failed commits")
			.labelNames("service","topic")
			.register();
	
	/**
	 * Summary of total number of messages returned by poll per topic.
	 */
	 private static Summary polledMessages = 
		Summary.build()
		    .name("innominds_msglib_polled_messages")
		    .help("Number of messages returned in poll")
	        .labelNames("service","topic")
	        .register();
	 
	 /**
	  * Summary of time spent in calls to poll method per topic.
	  */
	 private static Summary pollLatency = 
		Summary.build()
		    .name("innominds_msglib_poll_latency_seconds")
		    .help("Time spent in poll.")
	        .labelNames("service","topic")
	        .register();
	 
	 /**
	  * Summary of time spent in calls to processing received messages per topic.
	  */
	 private static Summary messageProcessingLatency = 
		Summary.build()
		    .name("innominds_msglib_message_process_latency_seconds")
	        .help("Time spent in processing messages.")
	        .labelNames("service","topic")
	        .register();
   
	 /**
		 * Configuration of kafka.
		 * <p>
		 * This method is used to set the Properties for the Consumer and Producer.
		 * kafka-config.properties from the file and the provided bootStrapServer,
		 * kafka-producer-config.properties --> for Producer.
		 * kafka-common-config.properties --> for common properties
		 * </p>
		 * <p>Note: This instantiation of Properties access, will be done only once.</p>
		 * 
		 * @param bootStrapServer  the bootStrapServer value.
		 * @param serviceName The service name, that is using the messaging service.
		 * @return boolean based on configuration provided
		 * 
		 */
	  public static void configKafaProperties(String bootStrapServer,String serviceName,Properties userConsumerProperties,Properties userProducerProperties) throws Exception{
		  if(StringUtils.isBlank(bootStrapServer) || StringUtils.isBlank(serviceName)){
			  LOGGER.debug("Invalid input parameters for bootStrapServer or serviceName, please enter valid parameters.");
			  throw new Exception("Invalid input parameters for bootStrapServer or serviceName");
		  }
		  //Store The ServiceName for future use.
		  MessagingHelper.serviceName = serviceName;
		  
		  LOGGER.debug("MessagingHelper - static() - start");
		  messagingProperties = new MessagingProperties();
		  try{
			  //Load all the default consumer properties from the kafka-consumer-config file.
			  consumerProperties = messagingProperties.getPropValues(MessagingConstants.CONSUMER_PROPERTY_FILE);
			  consumerProperties.put("bootstrap.servers", bootStrapServer);
			  consumerProperties.put("group.id", serviceName);
			  /**
			   * Check, the service user, injected consumer properties.
			   * If exists, the fetch the entered properties and override 
			   * entries in {consumerProperties}.
			   * Now, it is supported for following properties.
			   * 1. FETCH_MIN_BYTES = "fetch.min.bytes"
			   * 2. MAX_POLL_RECORDS = "max.poll.records"
			   * 3. MAX_POLL_INTERVAL_MS = "max.poll.interval.ms"
			   * 4. FETCH_MAX_WAIT_MS = "fetch.max.wait.ms";
			   */
			  if(userConsumerProperties != null && !userConsumerProperties.isEmpty())
				 overrideConsumerAndProducerProperties(userConsumerProperties, true, false); 
			  			 
		      
			  producerProperties = messagingProperties.getPropValues(MessagingConstants.PRODUCER_PROPERTY_FILE);
			  producerProperties.put("bootstrap.servers", bootStrapServer);
			  
			  /**
			   * Check, the service user, injected producer properties.
			   * If exists, the fetch the entered properties and override 
			   * entries in {producerProperties}.
			   * Now, it is supported for following properties.
			   * 1. BATCH_SIZE = "batch.size";
			   * 2. LINGER_MS = "linger.ms";
			   * 3. BUFFER_MEMORY = "buffer.memory";
			   */
			  if(userProducerProperties != null && !userProducerProperties.isEmpty())
				  overrideConsumerAndProducerProperties(userProducerProperties,false,true);
			  
		      
		     //Construct Common Properties File
		     commonProperties = messagingProperties.getPropValues(MessagingConstants.COMMON_PROPERTY_FILE);
		     
		     //Initialize KafkaProducer --> For Once
		     kafkaProducer = new KafkaProducer<String, String>(producerProperties);
		      
		    }catch(Exception e){
		      LOGGER.error("Exception in MessagingHelper - static : ",e);
		      throw e;
		    }
		  
	  }
  
  /**
   * For running the producer to send the message
   * @param topicName
   * @param messageObject
   * @throws Exception
   */
  public static void runProducer(String topicName, String messageObject,ProducerCallback topicProducerCallBack) throws Exception {
	  LOGGER.debug("MessagingHelper - runProducer() - start");

    if (!"".equalsIgnoreCase(topicName) && topicName != null) {
    	LOGGER.debug("MessagingHelper - runProducer() - topic - " , topicName);
    	LOGGER.debug("Message - ", messageObject.toString());
    	
    	Callback callback = new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if ( exception == null ) {
				    //  Message Published successfully.
					LOGGER.debug("Message published successfully on topic: " + topicName);
					publishedMessagesCounter.labels(serviceName, topicName, "success").inc();
					if ( topicProducerCallBack != null )
				        topicProducerCallBack.onSuccess(metadata.topic(),messageObject);
				} else {
					//  Message failed to publish
					LOGGER.error("Failed to publish message on topic: " + topicName, exception);
					publishedMessagesCounter.labels(serviceName, topicName, "failed").inc();
					if ( topicProducerCallBack != null )
					    topicProducerCallBack.onFailure(topicName, messageObject, exception);
				}
					 
			}
		};
		
		kafkaProducer.send(new ProducerRecord<String, String>(topicName, messageObject),callback);
    } else {
    	LOGGER.info(
          "Exception - MessagingHelper - runProducer() - Given topic or list of topics are not present, create relavant topic in Kafka");
    }
    LOGGER.debug("MessagingHelper - runProducer() - end");
  }

  /**
   * For running the consumer to get the message
   * @param topicName
   * @param callback
   * @throws Exception
   */
  public static synchronized void subcribeToTopic(String topicName, 
		  										  IConsumerCallback callback,
		  										  IConsumerStateCallback connectionStateCallBack) throws Exception {

	LOGGER.debug("MessagingHelper - runConsumer() - start");
    List<String> topics = new ArrayList<>();
	topics.add(topicName);
	subcribeToTopics(topics, callback, connectionStateCallBack);
	LOGGER.debug("MessagingHelper - runConsumer() - end ");
  }

  /**
   * This method is designed to 
   * 1. Construct the consumer from existing properties.
   * 2. Subscribes to the topic
   * 3. On message arrival, executes the call back from the 
   *    pool.
   * @param topicNames
   * @param callback
   */
  public static void subcribeToTopics(List<String> topicNames, 
		  							 IConsumerCallback callback,
		  							 IConsumerStateCallback connectionStateCallBack) {
    
	  	if ( callback==null ){ 
			LOGGER.error("consumeMessages called with null callback!");
			throw new NullPointerException("consumeMessages called with null callback!");
		}
		
	  		  	
	  	//Construct the consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
		
		LOGGER.debug("::::Consumer ");
	   
		Runnable r = () -> {
			
			String topic = topicNames.get(0);
			
			try{
				  java.util.function.Consumer<ConsumerRecord<String, String>> action = (record) -> {
				  LOGGER.debug("topic: " , record.topic());
				  LOGGER.debug("message: " ,record.value());
				  /**
				   * Declare a {Set}, to hold all the partitions, for the failed
				   * Messages, to be processed. This will hold only {partition}.
				   */
				  Set<Integer> failedPartitionSet = new HashSet<>();
      			  try{
      				  /**
      				   * Process The message payload, if it is true, means the message has been 
      				   * processed successfully. Then go ahead and commit the message.
      				   * The condition need to be valid for the message to be committed, follows -->
      				   * 1. The message is published to a topic, which belongs to a particular {Partition}.
      				   * 2. If the message failed to be processed by the {ConsumerCallBack} then
      				   *    it will be considered that, message is NOT eligible for {Commit}.Obviously.
      				   * 3. This means that the respective partition, has a failed message.
      				   * 4. Hence forth, all the messages arrived through the {poll} operation and belongs 
      				   *    to the same partition, will NOT be eligible for a {Commit}.
      				   * 5. So, to develop this logic, do collect, failed message's Partition
      				   * 6.Now, finally if the arrived messagePayload is processed successfully, then 
      				   *   the respective partition should not exist in the cache. Then, it should be eligible
      				   *   for a COMMIT.
      				   *     
      				   * All in all, on a partition, if a arrived message is failed to be processed by the 
      				   * the consumer callback, then ALL other arrived messages on this partition, will not be commited. 
      				   */
      				if(failedPartitionSet.isEmpty() || !failedPartitionSet.contains(record.partition())){
	      				  if(callback.message(record.topic(), record.value())){
	      					 try{
	      						 /**
	      						  * Commit offsets returned on the last {@link #poll(long) poll()} for all the 
	      						  * subscribed list of topics and partitions. This commits offsets only to Kafka.
	      						  */
	      						 consumer.commitSync(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)));
	      					}catch(CommitFailedException commFex){
	      						LOGGER.error("Message Commit failed... ",commFex);
	      						
	      						failedCommitsCounter.labels(serviceName, record.topic()).inc();
	      						//Send the appropriate information to the client.
	      						if(connectionStateCallBack != null)
	      							connectionStateCallBack.onConsumerStateFailure(topicNames.get(0), commFex);
	      					 }
	      					
	      				  }else{
	      					/**
	      					 * This record is failed to process by the ConsumerCallback
	      					 * Collect the failed message's partition in a {Set}
	      					 */
	      					LOGGER.debug("This record is failed to process by the ConsumerCallback. For topic  " + record.topic() + " on partition " + record.partition());
	      					//Collect the {partition}
	      					failedPartitionSet.add(record.partition());
	      					failedMessagesCounter.labels(serviceName, record.topic()).inc();
	      				  }
      				}else{
						 /**
						  * This else loop says that, one message processing has already been 
						  * failed to process for this {partition}. Hence forth, further any other
						  * consumed message, that comes from the same partition, should not be 
						  * NOT eligible for processing at all.
						  */
							LOGGER.debug("Message processing restricted, because on this partition, at least one message has already failed to process. ");
							ignoredMessagesCounter.labels(serviceName, record.topic()).inc();
					 }
      				  
      				 
      			  }catch(Exception e){
					LOGGER.error("Exception ", e);
      			  }
      		};

      		LOGGER.debug("Setting subscribed topics to: ",topicNames);
      		consumer.subscribe(topicNames);

	      	while(true){
	      			//Action Item --> Make this Configurable
	      			Summary.Timer pollTimer = pollLatency.labels(serviceName, topic).startTimer();
	      			String pollTime = getCommonProperties().getProperty(MessagingConstants.CONSUMER_POLL_TIME_MILLIS);
		            ConsumerRecords<String, String> records = consumer.poll(Integer.parseInt(pollTime));
		            pollTimer.observeDuration();
		            
		            int receivedMessagesCount = records.count();
		             polledMessages.labels(serviceName, topic).observe(receivedMessagesCount);
		            
		             if ( (records==null) || records.isEmpty() )
		                   continue;
		            	
		            	LOGGER.debug("Number of messages received in consumer thread: ",receivedMessagesCount);
		            	Summary.Timer messageProcessingTimer = messageProcessingLatency.labels(serviceName, topic).startTimer();
		                records.forEach(action);
		                messageProcessingTimer.observeDuration();
		     }
				
	        }catch(WakeupException ex){
	        	LOGGER.error("WakeupException received! Exiting consumer I/O thread.");
	           	// Ignore exception if closing
	            if (!closed.get())  {
	            	LOGGER.error("WakeupException received without shutdown!",ex);
	            	throw ex;
	            }
	        }catch(KafkaException kex){
	        	// Send these information to the client, by calling the callback...
	        	LOGGER.error("Consumer connection state has been lost... ",kex);
	        	if(connectionStateCallBack != null)
	        		connectionStateCallBack.onConsumerStateFailure(null, kex);
	        }catch(IllegalStateException isex){
	        	// If the consumer is not subscribed to any topics, during the time of polling
	        	LOGGER.error("Consumer is not subscribed to any topics... ",isex);
	        	if(connectionStateCallBack != null)
	        		connectionStateCallBack.onConsumerStateFailure(null, isex);
	        }catch(Exception ex){
	        	//For any other unrecoverable errors
	        	LOGGER.error("Caunght an unexpected exception in consumer I/O thread!",ex);
	        	if(connectionStateCallBack != null)
	        		connectionStateCallBack.onConsumerStateFailure(topicNames.get(0), ex);
	        }finally{
	             consumer.close();
	        }
		};
		
		consumersCounter.labels(serviceName).inc();
		
		//Assign the consumer runnable to a pool
		assignedConsumerRunnableToPool(r);
		LOGGER.debug("Starting consumer thread to read messages from subscribed topics");
		
   }

  public static Properties getConsumerProperties(){
		return consumerProperties;
  }
  
  public static Properties getCommonProperties(){
		return commonProperties;
}
  
  public static synchronized void assignedConsumerRunnableToPool(Runnable consumer) {
	   if(consumerThreadPool == null){
		  String maxSize = getCommonProperties().getProperty(MessagingConstants.CONSUMER_POOL_MAX_SIZE);
		  consumerThreadPool =  Executors.newFixedThreadPool(Integer.parseInt(maxSize)); 
	   }
	   
	   consumerThreadPool.submit(consumer);
  }
  /**
   * Just to override the existing applicable properties.
   * @param userConsumerProperties
   * @param isConsumer 
   * @param isProducer
   */
  private static void overrideConsumerAndProducerProperties(Properties userConsumerOrProducerProperties,boolean isConsumer,boolean isProducer){
	//If it is for consumer properties, do override properties appropriately
	  if(isConsumer){
		  Set consumerPropertiesSet = userConsumerOrProducerProperties.entrySet();
		  Iterator<String> itr = consumerPropertiesSet.iterator();
		  if(itr.hasNext()){
			  String propertykey = itr.next();
			  if(propertykey.equals(MessagingConstants.FETCH_MIN_BYTES)){
				  consumerProperties.put(MessagingConstants.FETCH_MIN_BYTES, userConsumerOrProducerProperties.get(MessagingConstants.FETCH_MIN_BYTES)); 
			  }else if(propertykey.equals(MessagingConstants.MAX_POLL_RECORDS)){
				  consumerProperties.put(MessagingConstants.MAX_POLL_RECORDS, userConsumerOrProducerProperties.get(MessagingConstants.MAX_POLL_RECORDS)); 
			  }else if(propertykey.equals(MessagingConstants.MAX_POLL_INTERVAL_MS)){
				  consumerProperties.put(MessagingConstants.MAX_POLL_INTERVAL_MS, userConsumerOrProducerProperties.get(MessagingConstants.MAX_POLL_INTERVAL_MS)); 
			  }else if(propertykey.equals(MessagingConstants.FETCH_MAX_WAIT_MS)){
				  consumerProperties.put(MessagingConstants.FETCH_MAX_WAIT_MS, userConsumerOrProducerProperties.get(MessagingConstants.FETCH_MAX_WAIT_MS)); 
			  }else{
				  LOGGER.debug("This proeprty is not required for consumer");
			  }
		  }
	  }
	  
	  //If it is for producer properties, do override properties appropriately
	  if(isProducer){
		  Set producerPropertiesSet = userConsumerOrProducerProperties.entrySet();
		  Iterator<String> itr = producerPropertiesSet.iterator();
		  if(itr.hasNext()){
			  String propertykey = itr.next();
			  if(propertykey.equals(MessagingConstants.BATCH_SIZE)){
				  producerProperties.put(MessagingConstants.BATCH_SIZE, userConsumerOrProducerProperties.get(MessagingConstants.BATCH_SIZE)); 
			  }else if(propertykey.equals(MessagingConstants.LINGER_MS)){
				  producerProperties.put(MessagingConstants.LINGER_MS, userConsumerOrProducerProperties.get(MessagingConstants.LINGER_MS)); 
			  }else if(propertykey.equals(MessagingConstants.BUFFER_MEMORY)){
				  producerProperties.put(MessagingConstants.BUFFER_MEMORY, userConsumerOrProducerProperties.get(MessagingConstants.BUFFER_MEMORY)); 
			  }else{
				  LOGGER.debug("This proeprty is not required for producer");
			  }
		  } 
	  }
  }
}