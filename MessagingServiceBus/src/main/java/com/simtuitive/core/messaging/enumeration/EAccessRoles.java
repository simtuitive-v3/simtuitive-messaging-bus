package com.simtuitive.core.messaging.enumeration;

/**
 *
 * The {@link EAccessRoles} enum defines Access Roles enumerations which are used in the implementation library for
 * knowing the role status. Ex: Produce, Consume and ProduceConsume
 *
 */
public enum EAccessRoles {
  eCanProduce(0), // Only produce messages
  eCanConsume(1), // Only consume messages
  eCanProduceAndConsume(2); // Access both produce and consume messages

  // This field can hold the value of accessRole and used in the library to get the value
  private int accessRole;

  /**
   * @return the accessRole
   */
  public int getAccessRole() {
    return accessRole;
  }

  /**
   * @param accessRole the accessRole to set
   */
  EAccessRoles(int accessRole) {
    this.accessRole = accessRole;
  }

}