/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.hiido.hva.thrift.protocol;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum Recode implements TEnum {
  SUCESS(0),
  FAILURE(1);

  private final int value;

  private Recode(int value) {
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
  public static Recode findByValue(int value) { 
    switch (value) {
      case 0:
        return SUCESS;
      case 1:
        return FAILURE;
      default:
        return null;
    }
  }
}
