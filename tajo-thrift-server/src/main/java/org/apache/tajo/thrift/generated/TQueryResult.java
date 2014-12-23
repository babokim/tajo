/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.tajo.thrift.generated;

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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2014-12-20")
public class TQueryResult implements org.apache.thrift.TBase<TQueryResult, TQueryResult._Fields>, java.io.Serializable, Cloneable, Comparable<TQueryResult> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TQueryResult");

  private static final org.apache.thrift.protocol.TField TABLE_DESC_FIELD_DESC = new org.apache.thrift.protocol.TField("tableDesc", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField ROWS_FIELD_DESC = new org.apache.thrift.protocol.TField("rows", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField SCHEMA_FIELD_DESC = new org.apache.thrift.protocol.TField("schema", org.apache.thrift.protocol.TType.STRUCT, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TQueryResultStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TQueryResultTupleSchemeFactory());
  }

  public TTableDesc tableDesc; // required
  public List<TRowData> rows; // required
  public TSchema schema; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_DESC((short)1, "tableDesc"),
    ROWS((short)2, "rows"),
    SCHEMA((short)3, "schema");

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
        case 1: // TABLE_DESC
          return TABLE_DESC;
        case 2: // ROWS
          return ROWS;
        case 3: // SCHEMA
          return SCHEMA;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TABLE_DESC, new org.apache.thrift.meta_data.FieldMetaData("tableDesc", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TTableDesc.class)));
    tmpMap.put(_Fields.ROWS, new org.apache.thrift.meta_data.FieldMetaData("rows", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TRowData.class))));
    tmpMap.put(_Fields.SCHEMA, new org.apache.thrift.meta_data.FieldMetaData("schema", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSchema.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TQueryResult.class, metaDataMap);
  }

  public TQueryResult() {
  }

  public TQueryResult(
    TTableDesc tableDesc,
    List<TRowData> rows,
    TSchema schema)
  {
    this();
    this.tableDesc = tableDesc;
    this.rows = rows;
    this.schema = schema;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TQueryResult(TQueryResult other) {
    if (other.isSetTableDesc()) {
      this.tableDesc = new TTableDesc(other.tableDesc);
    }
    if (other.isSetRows()) {
      List<TRowData> __this__rows = new ArrayList<TRowData>(other.rows.size());
      for (TRowData other_element : other.rows) {
        __this__rows.add(new TRowData(other_element));
      }
      this.rows = __this__rows;
    }
    if (other.isSetSchema()) {
      this.schema = new TSchema(other.schema);
    }
  }

  public TQueryResult deepCopy() {
    return new TQueryResult(this);
  }

  @Override
  public void clear() {
    this.tableDesc = null;
    this.rows = null;
    this.schema = null;
  }

  public TTableDesc getTableDesc() {
    return this.tableDesc;
  }

  public TQueryResult setTableDesc(TTableDesc tableDesc) {
    this.tableDesc = tableDesc;
    return this;
  }

  public void unsetTableDesc() {
    this.tableDesc = null;
  }

  /** Returns true if field tableDesc is set (has been assigned a value) and false otherwise */
  public boolean isSetTableDesc() {
    return this.tableDesc != null;
  }

  public void setTableDescIsSet(boolean value) {
    if (!value) {
      this.tableDesc = null;
    }
  }

  public int getRowsSize() {
    return (this.rows == null) ? 0 : this.rows.size();
  }

  public java.util.Iterator<TRowData> getRowsIterator() {
    return (this.rows == null) ? null : this.rows.iterator();
  }

  public void addToRows(TRowData elem) {
    if (this.rows == null) {
      this.rows = new ArrayList<TRowData>();
    }
    this.rows.add(elem);
  }

  public List<TRowData> getRows() {
    return this.rows;
  }

  public TQueryResult setRows(List<TRowData> rows) {
    this.rows = rows;
    return this;
  }

  public void unsetRows() {
    this.rows = null;
  }

  /** Returns true if field rows is set (has been assigned a value) and false otherwise */
  public boolean isSetRows() {
    return this.rows != null;
  }

  public void setRowsIsSet(boolean value) {
    if (!value) {
      this.rows = null;
    }
  }

  public TSchema getSchema() {
    return this.schema;
  }

  public TQueryResult setSchema(TSchema schema) {
    this.schema = schema;
    return this;
  }

  public void unsetSchema() {
    this.schema = null;
  }

  /** Returns true if field schema is set (has been assigned a value) and false otherwise */
  public boolean isSetSchema() {
    return this.schema != null;
  }

  public void setSchemaIsSet(boolean value) {
    if (!value) {
      this.schema = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TABLE_DESC:
      if (value == null) {
        unsetTableDesc();
      } else {
        setTableDesc((TTableDesc)value);
      }
      break;

    case ROWS:
      if (value == null) {
        unsetRows();
      } else {
        setRows((List<TRowData>)value);
      }
      break;

    case SCHEMA:
      if (value == null) {
        unsetSchema();
      } else {
        setSchema((TSchema)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_DESC:
      return getTableDesc();

    case ROWS:
      return getRows();

    case SCHEMA:
      return getSchema();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TABLE_DESC:
      return isSetTableDesc();
    case ROWS:
      return isSetRows();
    case SCHEMA:
      return isSetSchema();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TQueryResult)
      return this.equals((TQueryResult)that);
    return false;
  }

  public boolean equals(TQueryResult that) {
    if (that == null)
      return false;

    boolean this_present_tableDesc = true && this.isSetTableDesc();
    boolean that_present_tableDesc = true && that.isSetTableDesc();
    if (this_present_tableDesc || that_present_tableDesc) {
      if (!(this_present_tableDesc && that_present_tableDesc))
        return false;
      if (!this.tableDesc.equals(that.tableDesc))
        return false;
    }

    boolean this_present_rows = true && this.isSetRows();
    boolean that_present_rows = true && that.isSetRows();
    if (this_present_rows || that_present_rows) {
      if (!(this_present_rows && that_present_rows))
        return false;
      if (!this.rows.equals(that.rows))
        return false;
    }

    boolean this_present_schema = true && this.isSetSchema();
    boolean that_present_schema = true && that.isSetSchema();
    if (this_present_schema || that_present_schema) {
      if (!(this_present_schema && that_present_schema))
        return false;
      if (!this.schema.equals(that.schema))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_tableDesc = true && (isSetTableDesc());
    list.add(present_tableDesc);
    if (present_tableDesc)
      list.add(tableDesc);

    boolean present_rows = true && (isSetRows());
    list.add(present_rows);
    if (present_rows)
      list.add(rows);

    boolean present_schema = true && (isSetSchema());
    list.add(present_schema);
    if (present_schema)
      list.add(schema);

    return list.hashCode();
  }

  @Override
  public int compareTo(TQueryResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTableDesc()).compareTo(other.isSetTableDesc());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableDesc()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableDesc, other.tableDesc);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRows()).compareTo(other.isSetRows());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRows()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.rows, other.rows);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSchema()).compareTo(other.isSetSchema());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSchema()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.schema, other.schema);
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
    StringBuilder sb = new StringBuilder("TQueryResult(");
    boolean first = true;

    sb.append("tableDesc:");
    if (this.tableDesc == null) {
      sb.append("null");
    } else {
      sb.append(this.tableDesc);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("rows:");
    if (this.rows == null) {
      sb.append("null");
    } else {
      sb.append(this.rows);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("schema:");
    if (this.schema == null) {
      sb.append("null");
    } else {
      sb.append(this.schema);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check for sub-struct validity
    if (tableDesc != null) {
      tableDesc.validate();
    }
    if (schema != null) {
      schema.validate();
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TQueryResultStandardSchemeFactory implements SchemeFactory {
    public TQueryResultStandardScheme getScheme() {
      return new TQueryResultStandardScheme();
    }
  }

  private static class TQueryResultStandardScheme extends StandardScheme<TQueryResult> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TQueryResult struct) throws TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TABLE_DESC
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.tableDesc = new TTableDesc();
              struct.tableDesc.read(iprot);
              struct.setTableDescIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ROWS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list34 = iprot.readListBegin();
                struct.rows = new ArrayList<TRowData>(_list34.size);
                TRowData _elem35;
                for (int _i36 = 0; _i36 < _list34.size; ++_i36)
                {
                  _elem35 = new TRowData();
                  _elem35.read(iprot);
                  struct.rows.add(_elem35);
                }
                iprot.readListEnd();
              }
              struct.setRowsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SCHEMA
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.schema = new TSchema();
              struct.schema.read(iprot);
              struct.setSchemaIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TQueryResult struct) throws TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableDesc != null) {
        oprot.writeFieldBegin(TABLE_DESC_FIELD_DESC);
        struct.tableDesc.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.rows != null) {
        oprot.writeFieldBegin(ROWS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.rows.size()));
          for (TRowData _iter37 : struct.rows)
          {
            _iter37.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.schema != null) {
        oprot.writeFieldBegin(SCHEMA_FIELD_DESC);
        struct.schema.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TQueryResultTupleSchemeFactory implements SchemeFactory {
    public TQueryResultTupleScheme getScheme() {
      return new TQueryResultTupleScheme();
    }
  }

  private static class TQueryResultTupleScheme extends TupleScheme<TQueryResult> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TQueryResult struct) throws TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetTableDesc()) {
        optionals.set(0);
      }
      if (struct.isSetRows()) {
        optionals.set(1);
      }
      if (struct.isSetSchema()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetTableDesc()) {
        struct.tableDesc.write(oprot);
      }
      if (struct.isSetRows()) {
        {
          oprot.writeI32(struct.rows.size());
          for (TRowData _iter38 : struct.rows)
          {
            _iter38.write(oprot);
          }
        }
      }
      if (struct.isSetSchema()) {
        struct.schema.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TQueryResult struct) throws TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.tableDesc = new TTableDesc();
        struct.tableDesc.read(iprot);
        struct.setTableDescIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list39 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.rows = new ArrayList<TRowData>(_list39.size);
          TRowData _elem40;
          for (int _i41 = 0; _i41 < _list39.size; ++_i41)
          {
            _elem40 = new TRowData();
            _elem40.read(iprot);
            struct.rows.add(_elem40);
          }
        }
        struct.setRowsIsSet(true);
      }
      if (incoming.get(2)) {
        struct.schema = new TSchema();
        struct.schema.read(iprot);
        struct.setSchemaIsSet(true);
      }
    }
  }

}
