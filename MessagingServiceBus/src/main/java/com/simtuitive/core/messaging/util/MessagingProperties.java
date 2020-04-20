package com.simtuitive.core.messaging.util;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.simtuitive.core.messaging.constants.MessagingConstants;

/**
 *
 * The {@link MessagingProperties} class for loading the properties of Kafka and to save them in the properties for
 * usage of produce and consume operations
 *
 */
public class MessagingProperties {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessagingProperties.class);

  /**
   * For loading all the kafka properties.
   * @param propertyFile specifies incoming property file name
   * 		[consumerConfigFileName, producerConfigFileName,commonConfigFileName]
   * @return
   * @throws Exception
   */
  public Properties getPropValues(String propertyFile) throws Exception {
	LOGGER.info("MessagingProperties - getPropValues() - start");
    Properties properties = null;
    InputStream inputStream = null;
    try {
      properties = new Properties();
      LOGGER.info("The properties are going to be loaded from file name :  " , propertyFile);

      inputStream = getClass().getClassLoader().getResourceAsStream(propertyFile);

      if(inputStream != null){
          properties.load(inputStream);
      }else{
    	LOGGER.error("Property file not found in the classpath",propertyFile);
        throw new FileNotFoundException("Property file '" + propertyFile + "' not found in the classpath");
      }
    }catch(Exception e){
      LOGGER.error("Exception in MessagingProperties - getPropValues() : " , e.getMessage());
      throw new Exception("Exception in MessagingProperties - getPropValues() : " + e.getMessage());
    }finally{
      inputStream.close();
    }

    LOGGER.info("MessagingProperties - getPropValues() - end");

    return properties;
  }
  
 }