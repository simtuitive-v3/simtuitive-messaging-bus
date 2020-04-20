package com.simtuitive.core.messaging.util;

import com.simtuitive.core.messaging.enumeration.EResponseCode;

/**
 *
 * Class details
 * 
 */
public class MessagingServiceException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * 
   */
  public MessagingServiceException() {
    super();
  }

  /**
   * @param edeniedaccess
   */
  public MessagingServiceException(EResponseCode responseCode) {
    super(responseCode.toString());
  }

}
