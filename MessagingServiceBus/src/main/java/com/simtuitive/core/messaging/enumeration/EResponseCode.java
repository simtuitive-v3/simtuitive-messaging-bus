package com.simtuitive.core.messaging.enumeration;

/**
 *
 * The {@link EResponseCode} enum defines the responseCode that will provide the status of operation.
 *
 */
public enum EResponseCode {
  eSuccess(0), // Succesful Operation
  eDeniedAccess(1), // Access Denied to publish/consume or both
  eServiceFailure(2); // Kafka Service Failure - Service Unavailability

  private int responseCode;

  /**
   * @return the responseCode
   */
  public int getResponseCode() {
    return responseCode;
  }

  /**
   * @param responseCode the responseCode to set
   */
  EResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Enum#toString()
   */
  @Override
  public String toString() {
    return String.valueOf(responseCode);
  }
}
