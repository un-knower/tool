/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.hiido.hcat.thrift.protocol;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2016-06-23")
public class LoadFile implements org.apache.thrift.TBase<LoadFile, LoadFile._Fields>, java.io.Serializable, Cloneable, Comparable<LoadFile> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LoadFile");

  private static final org.apache.thrift.protocol.TField CIPHER_FIELD_DESC = new org.apache.thrift.protocol.TField("cipher", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField QUERY_FIELD_DESC = new org.apache.thrift.protocol.TField("query", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField CURRENT_DATABASE_FIELD_DESC = new org.apache.thrift.protocol.TField("currentDatabase", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField CONF_FIELD_DESC = new org.apache.thrift.protocol.TField("conf", org.apache.thrift.protocol.TType.MAP, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LoadFileStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LoadFileTupleSchemeFactory());
  }

  public Map<String,String> cipher; // required
  public String query; // required
  public String currentDatabase; // required
  public Map<String,String> conf; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    CIPHER((short)1, "cipher"),
    QUERY((short)2, "query"),
    CURRENT_DATABASE((short)3, "currentDatabase"),
    CONF((short)4, "conf");

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
        case 1: // CIPHER
          return CIPHER;
        case 2: // QUERY
          return QUERY;
        case 3: // CURRENT_DATABASE
          return CURRENT_DATABASE;
        case 4: // CONF
          return CONF;
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
  private static final _Fields optionals[] = {_Fields.CONF};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.CIPHER, new org.apache.thrift.meta_data.FieldMetaData("cipher", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.QUERY, new org.apache.thrift.meta_data.FieldMetaData("query", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CURRENT_DATABASE, new org.apache.thrift.meta_data.FieldMetaData("currentDatabase", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CONF, new org.apache.thrift.meta_data.FieldMetaData("conf", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LoadFile.class, metaDataMap);
  }

  public LoadFile() {
  }

  public LoadFile(
    Map<String,String> cipher,
    String query,
    String currentDatabase)
  {
    this();
    this.cipher = cipher;
    this.query = query;
    this.currentDatabase = currentDatabase;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LoadFile(LoadFile other) {
    if (other.isSetCipher()) {
      Map<String,String> __this__cipher = new HashMap<String,String>(other.cipher);
      this.cipher = __this__cipher;
    }
    if (other.isSetQuery()) {
      this.query = other.query;
    }
    if (other.isSetCurrentDatabase()) {
      this.currentDatabase = other.currentDatabase;
    }
    if (other.isSetConf()) {
      Map<String,String> __this__conf = new HashMap<String,String>(other.conf);
      this.conf = __this__conf;
    }
  }

  public LoadFile deepCopy() {
    return new LoadFile(this);
  }

  @Override
  public void clear() {
    this.cipher = null;
    this.query = null;
    this.currentDatabase = null;
    this.conf = null;
  }

  public int getCipherSize() {
    return (this.cipher == null) ? 0 : this.cipher.size();
  }

  public void putToCipher(String key, String val) {
    if (this.cipher == null) {
      this.cipher = new HashMap<String,String>();
    }
    this.cipher.put(key, val);
  }

  public Map<String,String> getCipher() {
    return this.cipher;
  }

  public LoadFile setCipher(Map<String,String> cipher) {
    this.cipher = cipher;
    return this;
  }

  public void unsetCipher() {
    this.cipher = null;
  }

  /** Returns true if field cipher is set (has been assigned a value) and false otherwise */
  public boolean isSetCipher() {
    return this.cipher != null;
  }

  public void setCipherIsSet(boolean value) {
    if (!value) {
      this.cipher = null;
    }
  }

  public String getQuery() {
    return this.query;
  }

  public LoadFile setQuery(String query) {
    this.query = query;
    return this;
  }

  public void unsetQuery() {
    this.query = null;
  }

  /** Returns true if field query is set (has been assigned a value) and false otherwise */
  public boolean isSetQuery() {
    return this.query != null;
  }

  public void setQueryIsSet(boolean value) {
    if (!value) {
      this.query = null;
    }
  }

  public String getCurrentDatabase() {
    return this.currentDatabase;
  }

  public LoadFile setCurrentDatabase(String currentDatabase) {
    this.currentDatabase = currentDatabase;
    return this;
  }

  public void unsetCurrentDatabase() {
    this.currentDatabase = null;
  }

  /** Returns true if field currentDatabase is set (has been assigned a value) and false otherwise */
  public boolean isSetCurrentDatabase() {
    return this.currentDatabase != null;
  }

  public void setCurrentDatabaseIsSet(boolean value) {
    if (!value) {
      this.currentDatabase = null;
    }
  }

  public int getConfSize() {
    return (this.conf == null) ? 0 : this.conf.size();
  }

  public void putToConf(String key, String val) {
    if (this.conf == null) {
      this.conf = new HashMap<String,String>();
    }
    this.conf.put(key, val);
  }

  public Map<String,String> getConf() {
    return this.conf;
  }

  public LoadFile setConf(Map<String,String> conf) {
    this.conf = conf;
    return this;
  }

  public void unsetConf() {
    this.conf = null;
  }

  /** Returns true if field conf is set (has been assigned a value) and false otherwise */
  public boolean isSetConf() {
    return this.conf != null;
  }

  public void setConfIsSet(boolean value) {
    if (!value) {
      this.conf = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case CIPHER:
      if (value == null) {
        unsetCipher();
      } else {
        setCipher((Map<String,String>)value);
      }
      break;

    case QUERY:
      if (value == null) {
        unsetQuery();
      } else {
        setQuery((String)value);
      }
      break;

    case CURRENT_DATABASE:
      if (value == null) {
        unsetCurrentDatabase();
      } else {
        setCurrentDatabase((String)value);
      }
      break;

    case CONF:
      if (value == null) {
        unsetConf();
      } else {
        setConf((Map<String,String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case CIPHER:
      return getCipher();

    case QUERY:
      return getQuery();

    case CURRENT_DATABASE:
      return getCurrentDatabase();

    case CONF:
      return getConf();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case CIPHER:
      return isSetCipher();
    case QUERY:
      return isSetQuery();
    case CURRENT_DATABASE:
      return isSetCurrentDatabase();
    case CONF:
      return isSetConf();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LoadFile)
      return this.equals((LoadFile)that);
    return false;
  }

  public boolean equals(LoadFile that) {
    if (that == null)
      return false;

    boolean this_present_cipher = true && this.isSetCipher();
    boolean that_present_cipher = true && that.isSetCipher();
    if (this_present_cipher || that_present_cipher) {
      if (!(this_present_cipher && that_present_cipher))
        return false;
      if (!this.cipher.equals(that.cipher))
        return false;
    }

    boolean this_present_query = true && this.isSetQuery();
    boolean that_present_query = true && that.isSetQuery();
    if (this_present_query || that_present_query) {
      if (!(this_present_query && that_present_query))
        return false;
      if (!this.query.equals(that.query))
        return false;
    }

    boolean this_present_currentDatabase = true && this.isSetCurrentDatabase();
    boolean that_present_currentDatabase = true && that.isSetCurrentDatabase();
    if (this_present_currentDatabase || that_present_currentDatabase) {
      if (!(this_present_currentDatabase && that_present_currentDatabase))
        return false;
      if (!this.currentDatabase.equals(that.currentDatabase))
        return false;
    }

    boolean this_present_conf = true && this.isSetConf();
    boolean that_present_conf = true && that.isSetConf();
    if (this_present_conf || that_present_conf) {
      if (!(this_present_conf && that_present_conf))
        return false;
      if (!this.conf.equals(that.conf))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_cipher = true && (isSetCipher());
    list.add(present_cipher);
    if (present_cipher)
      list.add(cipher);

    boolean present_query = true && (isSetQuery());
    list.add(present_query);
    if (present_query)
      list.add(query);

    boolean present_currentDatabase = true && (isSetCurrentDatabase());
    list.add(present_currentDatabase);
    if (present_currentDatabase)
      list.add(currentDatabase);

    boolean present_conf = true && (isSetConf());
    list.add(present_conf);
    if (present_conf)
      list.add(conf);

    return list.hashCode();
  }

  @Override
  public int compareTo(LoadFile other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetCipher()).compareTo(other.isSetCipher());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCipher()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.cipher, other.cipher);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetQuery()).compareTo(other.isSetQuery());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQuery()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.query, other.query);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCurrentDatabase()).compareTo(other.isSetCurrentDatabase());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCurrentDatabase()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.currentDatabase, other.currentDatabase);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetConf()).compareTo(other.isSetConf());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConf()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.conf, other.conf);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("LoadFile(");
    boolean first = true;

    sb.append("cipher:");
    if (this.cipher == null) {
      sb.append("null");
    } else {
      sb.append(this.cipher);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("query:");
    if (this.query == null) {
      sb.append("null");
    } else {
      sb.append(this.query);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("currentDatabase:");
    if (this.currentDatabase == null) {
      sb.append("null");
    } else {
      sb.append(this.currentDatabase);
    }
    first = false;
    if (isSetConf()) {
      if (!first) sb.append(", ");
      sb.append("conf:");
      if (this.conf == null) {
        sb.append("null");
      } else {
        sb.append(this.conf);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (cipher == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'cipher' was not present! Struct: " + toString());
    }
    if (query == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'query' was not present! Struct: " + toString());
    }
    if (currentDatabase == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'currentDatabase' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class LoadFileStandardSchemeFactory implements SchemeFactory {
    public LoadFileStandardScheme getScheme() {
      return new LoadFileStandardScheme();
    }
  }

  private static class LoadFileStandardScheme extends StandardScheme<LoadFile> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LoadFile struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CIPHER
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map64 = iprot.readMapBegin();
                struct.cipher = new HashMap<String,String>(2*_map64.size);
                String _key65;
                String _val66;
                for (int _i67 = 0; _i67 < _map64.size; ++_i67)
                {
                  _key65 = iprot.readString();
                  _val66 = iprot.readString();
                  struct.cipher.put(_key65, _val66);
                }
                iprot.readMapEnd();
              }
              struct.setCipherIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // QUERY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.query = iprot.readString();
              struct.setQueryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CURRENT_DATABASE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.currentDatabase = iprot.readString();
              struct.setCurrentDatabaseIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // CONF
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map68 = iprot.readMapBegin();
                struct.conf = new HashMap<String,String>(2*_map68.size);
                String _key69;
                String _val70;
                for (int _i71 = 0; _i71 < _map68.size; ++_i71)
                {
                  _key69 = iprot.readString();
                  _val70 = iprot.readString();
                  struct.conf.put(_key69, _val70);
                }
                iprot.readMapEnd();
              }
              struct.setConfIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, LoadFile struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.cipher != null) {
        oprot.writeFieldBegin(CIPHER_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.cipher.size()));
          for (Map.Entry<String, String> _iter72 : struct.cipher.entrySet())
          {
            oprot.writeString(_iter72.getKey());
            oprot.writeString(_iter72.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.query != null) {
        oprot.writeFieldBegin(QUERY_FIELD_DESC);
        oprot.writeString(struct.query);
        oprot.writeFieldEnd();
      }
      if (struct.currentDatabase != null) {
        oprot.writeFieldBegin(CURRENT_DATABASE_FIELD_DESC);
        oprot.writeString(struct.currentDatabase);
        oprot.writeFieldEnd();
      }
      if (struct.conf != null) {
        if (struct.isSetConf()) {
          oprot.writeFieldBegin(CONF_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.conf.size()));
            for (Map.Entry<String, String> _iter73 : struct.conf.entrySet())
            {
              oprot.writeString(_iter73.getKey());
              oprot.writeString(_iter73.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LoadFileTupleSchemeFactory implements SchemeFactory {
    public LoadFileTupleScheme getScheme() {
      return new LoadFileTupleScheme();
    }
  }

  private static class LoadFileTupleScheme extends TupleScheme<LoadFile> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LoadFile struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.cipher.size());
        for (Map.Entry<String, String> _iter74 : struct.cipher.entrySet())
        {
          oprot.writeString(_iter74.getKey());
          oprot.writeString(_iter74.getValue());
        }
      }
      oprot.writeString(struct.query);
      oprot.writeString(struct.currentDatabase);
      BitSet optionals = new BitSet();
      if (struct.isSetConf()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetConf()) {
        {
          oprot.writeI32(struct.conf.size());
          for (Map.Entry<String, String> _iter75 : struct.conf.entrySet())
          {
            oprot.writeString(_iter75.getKey());
            oprot.writeString(_iter75.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LoadFile struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map76 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
        struct.cipher = new HashMap<String,String>(2*_map76.size);
        String _key77;
        String _val78;
        for (int _i79 = 0; _i79 < _map76.size; ++_i79)
        {
          _key77 = iprot.readString();
          _val78 = iprot.readString();
          struct.cipher.put(_key77, _val78);
        }
      }
      struct.setCipherIsSet(true);
      struct.query = iprot.readString();
      struct.setQueryIsSet(true);
      struct.currentDatabase = iprot.readString();
      struct.setCurrentDatabaseIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map80 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.conf = new HashMap<String,String>(2*_map80.size);
          String _key81;
          String _val82;
          for (int _i83 = 0; _i83 < _map80.size; ++_i83)
          {
            _key81 = iprot.readString();
            _val82 = iprot.readString();
            struct.conf.put(_key81, _val82);
          }
        }
        struct.setConfIsSet(true);
      }
    }
  }

}

