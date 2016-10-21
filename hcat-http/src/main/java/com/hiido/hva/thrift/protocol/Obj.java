/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.hiido.hva.thrift.protocol;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import javax.annotation.Generated;
import java.util.*;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
/**
 * 验证对象
 * 
 * name 对象名
 * type 对象类型，目前包括hive hdfs func
 * privilege 验证的权限，目前权限由4 bit 表示的 MRWX，例如需要验证对象的读写权限，privilege值为6
 * back1, back2 为备用字段，目前无任何对象类型使用（如有则在此备注）
 * 
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-10-21")
public class Obj implements org.apache.thrift.TBase<Obj, Obj._Fields>, java.io.Serializable, Cloneable, Comparable<Obj> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Obj");

  private static final org.apache.thrift.protocol.TField NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("name", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("type", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField PRIVILEGE_FIELD_DESC = new org.apache.thrift.protocol.TField("privilege", org.apache.thrift.protocol.TType.BYTE, (short)3);
  private static final org.apache.thrift.protocol.TField BACK1_FIELD_DESC = new org.apache.thrift.protocol.TField("back1", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField BACK2_FIELD_DESC = new org.apache.thrift.protocol.TField("back2", org.apache.thrift.protocol.TType.STRING, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ObjStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ObjTupleSchemeFactory());
  }

  public String name; // required
  public String type; // required
  public byte privilege; // required
  public String back1; // optional
  public String back2; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME((short)1, "name"),
    TYPE((short)2, "type"),
    PRIVILEGE((short)3, "privilege"),
    BACK1((short)4, "back1"),
    BACK2((short)5, "back2");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // NAME
          return NAME;
        case 2: // TYPE
          return TYPE;
        case 3: // PRIVILEGE
          return PRIVILEGE;
        case 4: // BACK1
          return BACK1;
        case 5: // BACK2
          return BACK2;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __PRIVILEGE_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.BACK1, _Fields.BACK2};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NAME, new org.apache.thrift.meta_data.FieldMetaData("name", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData("type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PRIVILEGE, new org.apache.thrift.meta_data.FieldMetaData("privilege", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BYTE)));
    tmpMap.put(_Fields.BACK1, new org.apache.thrift.meta_data.FieldMetaData("back1", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.BACK2, new org.apache.thrift.meta_data.FieldMetaData("back2", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Obj.class, metaDataMap);
  }

  public Obj() {
  }

  public Obj(
    String name,
    String type,
    byte privilege)
  {
    this();
    this.name = name;
    this.type = type;
    this.privilege = privilege;
    setPrivilegeIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Obj(Obj other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetType()) {
      this.type = other.type;
    }
    this.privilege = other.privilege;
    if (other.isSetBack1()) {
      this.back1 = other.back1;
    }
    if (other.isSetBack2()) {
      this.back2 = other.back2;
    }
  }

  public Obj deepCopy() {
    return new Obj(this);
  }

  @Override
  public void clear() {
    this.name = null;
    this.type = null;
    setPrivilegeIsSet(false);
    this.privilege = 0;
    this.back1 = null;
    this.back2 = null;
  }

  public String getName() {
    return this.name;
  }

  public Obj setName(String name) {
    this.name = name;
    return this;
  }

  public void unsetName() {
    this.name = null;
  }

  /** Returns true if field name is set (has been assigned a value) and false otherwise */
  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  public String getType() {
    return this.type;
  }

  public Obj setType(String type) {
    this.type = type;
    return this;
  }

  public void unsetType() {
    this.type = null;
  }

  /** Returns true if field type is set (has been assigned a value) and false otherwise */
  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
  }

  public byte getPrivilege() {
    return this.privilege;
  }

  public Obj setPrivilege(byte privilege) {
    this.privilege = privilege;
    setPrivilegeIsSet(true);
    return this;
  }

  public void unsetPrivilege() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PRIVILEGE_ISSET_ID);
  }

  /** Returns true if field privilege is set (has been assigned a value) and false otherwise */
  public boolean isSetPrivilege() {
    return EncodingUtils.testBit(__isset_bitfield, __PRIVILEGE_ISSET_ID);
  }

  public void setPrivilegeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PRIVILEGE_ISSET_ID, value);
  }

  public String getBack1() {
    return this.back1;
  }

  public Obj setBack1(String back1) {
    this.back1 = back1;
    return this;
  }

  public void unsetBack1() {
    this.back1 = null;
  }

  /** Returns true if field back1 is set (has been assigned a value) and false otherwise */
  public boolean isSetBack1() {
    return this.back1 != null;
  }

  public void setBack1IsSet(boolean value) {
    if (!value) {
      this.back1 = null;
    }
  }

  public String getBack2() {
    return this.back2;
  }

  public Obj setBack2(String back2) {
    this.back2 = back2;
    return this;
  }

  public void unsetBack2() {
    this.back2 = null;
  }

  /** Returns true if field back2 is set (has been assigned a value) and false otherwise */
  public boolean isSetBack2() {
    return this.back2 != null;
  }

  public void setBack2IsSet(boolean value) {
    if (!value) {
      this.back2 = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((String)value);
      }
      break;

    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((String)value);
      }
      break;

    case PRIVILEGE:
      if (value == null) {
        unsetPrivilege();
      } else {
        setPrivilege((Byte)value);
      }
      break;

    case BACK1:
      if (value == null) {
        unsetBack1();
      } else {
        setBack1((String)value);
      }
      break;

    case BACK2:
      if (value == null) {
        unsetBack2();
      } else {
        setBack2((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NAME:
      return getName();

    case TYPE:
      return getType();

    case PRIVILEGE:
      return getPrivilege();

    case BACK1:
      return getBack1();

    case BACK2:
      return getBack2();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NAME:
      return isSetName();
    case TYPE:
      return isSetType();
    case PRIVILEGE:
      return isSetPrivilege();
    case BACK1:
      return isSetBack1();
    case BACK2:
      return isSetBack2();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Obj)
      return this.equals((Obj)that);
    return false;
  }

  public boolean equals(Obj that) {
    if (that == null)
      return false;

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_privilege = true;
    boolean that_present_privilege = true;
    if (this_present_privilege || that_present_privilege) {
      if (!(this_present_privilege && that_present_privilege))
        return false;
      if (this.privilege != that.privilege)
        return false;
    }

    boolean this_present_back1 = true && this.isSetBack1();
    boolean that_present_back1 = true && that.isSetBack1();
    if (this_present_back1 || that_present_back1) {
      if (!(this_present_back1 && that_present_back1))
        return false;
      if (!this.back1.equals(that.back1))
        return false;
    }

    boolean this_present_back2 = true && this.isSetBack2();
    boolean that_present_back2 = true && that.isSetBack2();
    if (this_present_back2 || that_present_back2) {
      if (!(this_present_back2 && that_present_back2))
        return false;
      if (!this.back2.equals(that.back2))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_name = true && (isSetName());
    list.add(present_name);
    if (present_name)
      list.add(name);

    boolean present_type = true && (isSetType());
    list.add(present_type);
    if (present_type)
      list.add(type);

    boolean present_privilege = true;
    list.add(present_privilege);
    if (present_privilege)
      list.add(privilege);

    boolean present_back1 = true && (isSetBack1());
    list.add(present_back1);
    if (present_back1)
      list.add(back1);

    boolean present_back2 = true && (isSetBack2());
    list.add(present_back2);
    if (present_back2)
      list.add(back2);

    return list.hashCode();
  }

  @Override
  public int compareTo(Obj other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetName()).compareTo(other.isSetName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.name, other.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetType()).compareTo(other.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type, other.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPrivilege()).compareTo(other.isSetPrivilege());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPrivilege()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.privilege, other.privilege);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBack1()).compareTo(other.isSetBack1());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBack1()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.back1, other.back1);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBack2()).compareTo(other.isSetBack2());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBack2()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.back2, other.back2);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Obj(");
    boolean first = true;

    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("type:");
    if (this.type == null) {
      sb.append("null");
    } else {
      sb.append(this.type);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("privilege:");
    sb.append(this.privilege);
    first = false;
    if (isSetBack1()) {
      if (!first) sb.append(", ");
      sb.append("back1:");
      if (this.back1 == null) {
        sb.append("null");
      } else {
        sb.append(this.back1);
      }
      first = false;
    }
    if (isSetBack2()) {
      if (!first) sb.append(", ");
      sb.append("back2:");
      if (this.back2 == null) {
        sb.append("null");
      } else {
        sb.append(this.back2);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    if (name == null) {
      throw new TProtocolException("Required field 'name' was not present! Struct: " + toString());
    }
    if (type == null) {
      throw new TProtocolException("Required field 'type' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'privilege' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ObjStandardSchemeFactory implements SchemeFactory {
    public ObjStandardScheme getScheme() {
      return new ObjStandardScheme();
    }
  }

  private static class ObjStandardScheme extends StandardScheme<Obj> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Obj struct) throws TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.name = iprot.readString();
              struct.setNameIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.type = iprot.readString();
              struct.setTypeIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // PRIVILEGE
            if (schemeField.type == org.apache.thrift.protocol.TType.BYTE) {
              struct.privilege = iprot.readByte();
              struct.setPrivilegeIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // BACK1
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.back1 = iprot.readString();
              struct.setBack1IsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // BACK2
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.back2 = iprot.readString();
              struct.setBack2IsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetPrivilege()) {
        throw new TProtocolException("Required field 'privilege' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Obj struct) throws TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.name != null) {
        oprot.writeFieldBegin(NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }
      if (struct.type != null) {
        oprot.writeFieldBegin(TYPE_FIELD_DESC);
        oprot.writeString(struct.type);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(PRIVILEGE_FIELD_DESC);
      oprot.writeByte(struct.privilege);
      oprot.writeFieldEnd();
      if (struct.back1 != null) {
        if (struct.isSetBack1()) {
          oprot.writeFieldBegin(BACK1_FIELD_DESC);
          oprot.writeString(struct.back1);
          oprot.writeFieldEnd();
        }
      }
      if (struct.back2 != null) {
        if (struct.isSetBack2()) {
          oprot.writeFieldBegin(BACK2_FIELD_DESC);
          oprot.writeString(struct.back2);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ObjTupleSchemeFactory implements SchemeFactory {
    public ObjTupleScheme getScheme() {
      return new ObjTupleScheme();
    }
  }

  private static class ObjTupleScheme extends TupleScheme<Obj> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Obj struct) throws TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.name);
      oprot.writeString(struct.type);
      oprot.writeByte(struct.privilege);
      BitSet optionals = new BitSet();
      if (struct.isSetBack1()) {
        optionals.set(0);
      }
      if (struct.isSetBack2()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetBack1()) {
        oprot.writeString(struct.back1);
      }
      if (struct.isSetBack2()) {
        oprot.writeString(struct.back2);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Obj struct) throws TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.name = iprot.readString();
      struct.setNameIsSet(true);
      struct.type = iprot.readString();
      struct.setTypeIsSet(true);
      struct.privilege = iprot.readByte();
      struct.setPrivilegeIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.back1 = iprot.readString();
        struct.setBack1IsSet(true);
      }
      if (incoming.get(1)) {
        struct.back2 = iprot.readString();
        struct.setBack2IsSet(true);
      }
    }
  }

}

