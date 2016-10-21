/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.hiido.hva.thrift.protocol;


import org.apache.thrift.TEnum;

public enum RoleType implements TEnum {
  USER(0),
  TEAM(1);

  private final int value;

  private RoleType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static RoleType findByValue(int value) { 
    switch (value) {
      case 0:
        return USER;
      case 1:
        return TEAM;
      default:
        return null;
    }
  }
}
