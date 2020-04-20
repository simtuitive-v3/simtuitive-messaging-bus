package com.simtuitive.core.messaging.constants;

/**
 *
 * A {@link MessagingConstants} class for defining all the constants to be used in the implementation library
 *
 */
public class MessagingConstants {

  public static final String CONSUMER_PROPERTY_FILE = "kafka-consumer-config.properties";
  public static final String PRODUCER_PROPERTY_FILE = "kafka-producer-config.properties";
  public static final String COMMON_PROPERTY_FILE = "kafka-common-config.properties";
  public static final String ADMIN_AUTHORIZE_URL = "postRequestUrl";
  public static final String CONTENT_TYPE = "application/json";
  public static final Integer TOPIC_COUNT = 1;
  public static final String SERVICE_ID = "ID";
  public static final String SERVICE_NAME = "name";
  public static final String SERVICE_TOPIC = "topicName";
  public static final int RESPONSE_STATUS_CODE = 200;
  public static final String RESPONSE_ERROR_MESSAGE = "Failed : HTTP error code : ";
  public static final String CONSUMER_POLL_TIME_MILLIS = "consumer.poll.time.millis";
  public static final String CONSUMER_POOL_MAX_SIZE = "consumer.pool.max.size";
  //The consumer properties, service user can override
  public static final String FETCH_MIN_BYTES = "fetch.min.bytes";
  public static final String MAX_POLL_INTERVAL_MS = "max.poll.interval.ms";
  public static final String MAX_POLL_RECORDS = "max.poll.records";
  public static final String FETCH_MAX_WAIT_MS = "fetch.max.wait.ms";
//The producer properties, service user can override
  public static final String BATCH_SIZE = "batch.size";
  public static final String LINGER_MS = "linger.ms";
  public static final String BUFFER_MEMORY = "buffer.memory";
  
}
