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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2014-12-26")
public class TTableStats implements org.apache.thrift.TBase<TTableStats, TTableStats._Fields>, java.io.Serializable, Cloneable, Comparable<TTableStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TTableStats");

  private static final org.apache.thrift.protocol.TField NUM_ROWS_FIELD_DESC = new org.apache.thrift.protocol.TField("numRows", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField NUM_BYTES_FIELD_DESC = new org.apache.thrift.protocol.TField("numBytes", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField NUM_BLOCKS_FIELD_DESC = new org.apache.thrift.protocol.TField("numBlocks", org.apache.thrift.protocol.TType.I32, (short)3);
  private static final org.apache.thrift.protocol.TField NUM_SHUFFLE_OUTPUTS_FIELD_DESC = new org.apache.thrift.protocol.TField("numShuffleOutputs", org.apache.thrift.protocol.TType.I32, (short)4);
  private static final org.apache.thrift.protocol.TField AVG_ROWS_FIELD_DESC = new org.apache.thrift.protocol.TField("avgRows", org.apache.thrift.protocol.TType.I64, (short)5);
  private static final org.apache.thrift.protocol.TField READ_BYTES_FIELD_DESC = new org.apache.thrift.protocol.TField("readBytes", org.apache.thrift.protocol.TType.I64, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TTableStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TTableStatsTupleSchemeFactory());
  }

  public long numRows; // required
  public long numBytes; // required
  public int numBlocks; // required
  public int numShuffleOutputs; // required
  public long avgRows; // required
  public long readBytes; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NUM_ROWS((short)1, "numRows"),
    NUM_BYTES((short)2, "numBytes"),
    NUM_BLOCKS((short)3, "numBlocks"),
    NUM_SHUFFLE_OUTPUTS((short)4, "numShuffleOutputs"),
    AVG_ROWS((short)5, "avgRows"),
    READ_BYTES((short)6, "readBytes");

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
        case 1: // NUM_ROWS
          return NUM_ROWS;
        case 2: // NUM_BYTES
          return NUM_BYTES;
        case 3: // NUM_BLOCKS
          return NUM_BLOCKS;
        case 4: // NUM_SHUFFLE_OUTPUTS
          return NUM_SHUFFLE_OUTPUTS;
        case 5: // AVG_ROWS
          return AVG_ROWS;
        case 6: // READ_BYTES
          return READ_BYTES;
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
  private static final int __NUMROWS_ISSET_ID = 0;
  private static final int __NUMBYTES_ISSET_ID = 1;
  private static final int __NUMBLOCKS_ISSET_ID = 2;
  private static final int __NUMSHUFFLEOUTPUTS_ISSET_ID = 3;
  private static final int __AVGROWS_ISSET_ID = 4;
  private static final int __READBYTES_ISSET_ID = 5;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NUM_ROWS, new org.apache.thrift.meta_data.FieldMetaData("numRows", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.NUM_BYTES, new org.apache.thrift.meta_data.FieldMetaData("numBytes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.NUM_BLOCKS, new org.apache.thrift.meta_data.FieldMetaData("numBlocks", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.NUM_SHUFFLE_OUTPUTS, new org.apache.thrift.meta_data.FieldMetaData("numShuffleOutputs", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.AVG_ROWS, new org.apache.thrift.meta_data.FieldMetaData("avgRows", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.READ_BYTES, new org.apache.thrift.meta_data.FieldMetaData("readBytes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TTableStats.class, metaDataMap);
  }

  public TTableStats() {
  }

  public TTableStats(
    long numRows,
    long numBytes,
    int numBlocks,
    int numShuffleOutputs,
    long avgRows,
    long readBytes)
  {
    this();
    this.numRows = numRows;
    setNumRowsIsSet(true);
    this.numBytes = numBytes;
    setNumBytesIsSet(true);
    this.numBlocks = numBlocks;
    setNumBlocksIsSet(true);
    this.numShuffleOutputs = numShuffleOutputs;
    setNumShuffleOutputsIsSet(true);
    this.avgRows = avgRows;
    setAvgRowsIsSet(true);
    this.readBytes = readBytes;
    setReadBytesIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TTableStats(TTableStats other) {
    __isset_bitfield = other.__isset_bitfield;
    this.numRows = other.numRows;
    this.numBytes = other.numBytes;
    this.numBlocks = other.numBlocks;
    this.numShuffleOutputs = other.numShuffleOutputs;
    this.avgRows = other.avgRows;
    this.readBytes = other.readBytes;
  }

  public TTableStats deepCopy() {
    return new TTableStats(this);
  }

  @Override
  public void clear() {
    setNumRowsIsSet(false);
    this.numRows = 0;
    setNumBytesIsSet(false);
    this.numBytes = 0;
    setNumBlocksIsSet(false);
    this.numBlocks = 0;
    setNumShuffleOutputsIsSet(false);
    this.numShuffleOutputs = 0;
    setAvgRowsIsSet(false);
    this.avgRows = 0;
    setReadBytesIsSet(false);
    this.readBytes = 0;
  }

  public long getNumRows() {
    return this.numRows;
  }

  public TTableStats setNumRows(long numRows) {
    this.numRows = numRows;
    setNumRowsIsSet(true);
    return this;
  }

  public void unsetNumRows() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUMROWS_ISSET_ID);
  }

  /** Returns true if field numRows is set (has been assigned a value) and false otherwise */
  public boolean isSetNumRows() {
    return EncodingUtils.testBit(__isset_bitfield, __NUMROWS_ISSET_ID);
  }

  public void setNumRowsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUMROWS_ISSET_ID, value);
  }

  public long getNumBytes() {
    return this.numBytes;
  }

  public TTableStats setNumBytes(long numBytes) {
    this.numBytes = numBytes;
    setNumBytesIsSet(true);
    return this;
  }

  public void unsetNumBytes() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUMBYTES_ISSET_ID);
  }

  /** Returns true if field numBytes is set (has been assigned a value) and false otherwise */
  public boolean isSetNumBytes() {
    return EncodingUtils.testBit(__isset_bitfield, __NUMBYTES_ISSET_ID);
  }

  public void setNumBytesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUMBYTES_ISSET_ID, value);
  }

  public int getNumBlocks() {
    return this.numBlocks;
  }

  public TTableStats setNumBlocks(int numBlocks) {
    this.numBlocks = numBlocks;
    setNumBlocksIsSet(true);
    return this;
  }

  public void unsetNumBlocks() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUMBLOCKS_ISSET_ID);
  }

  /** Returns true if field numBlocks is set (has been assigned a value) and false otherwise */
  public boolean isSetNumBlocks() {
    return EncodingUtils.testBit(__isset_bitfield, __NUMBLOCKS_ISSET_ID);
  }

  public void setNumBlocksIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUMBLOCKS_ISSET_ID, value);
  }

  public int getNumShuffleOutputs() {
    return this.numShuffleOutputs;
  }

  public TTableStats setNumShuffleOutputs(int numShuffleOutputs) {
    this.numShuffleOutputs = numShuffleOutputs;
    setNumShuffleOutputsIsSet(true);
    return this;
  }

  public void unsetNumShuffleOutputs() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUMSHUFFLEOUTPUTS_ISSET_ID);
  }

  /** Returns true if field numShuffleOutputs is set (has been assigned a value) and false otherwise */
  public boolean isSetNumShuffleOutputs() {
    return EncodingUtils.testBit(__isset_bitfield, __NUMSHUFFLEOUTPUTS_ISSET_ID);
  }

  public void setNumShuffleOutputsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUMSHUFFLEOUTPUTS_ISSET_ID, value);
  }

  public long getAvgRows() {
    return this.avgRows;
  }

  public TTableStats setAvgRows(long avgRows) {
    this.avgRows = avgRows;
    setAvgRowsIsSet(true);
    return this;
  }

  public void unsetAvgRows() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __AVGROWS_ISSET_ID);
  }

  /** Returns true if field avgRows is set (has been assigned a value) and false otherwise */
  public boolean isSetAvgRows() {
    return EncodingUtils.testBit(__isset_bitfield, __AVGROWS_ISSET_ID);
  }

  public void setAvgRowsIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __AVGROWS_ISSET_ID, value);
  }

  public long getReadBytes() {
    return this.readBytes;
  }

  public TTableStats setReadBytes(long readBytes) {
    this.readBytes = readBytes;
    setReadBytesIsSet(true);
    return this;
  }

  public void unsetReadBytes() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __READBYTES_ISSET_ID);
  }

  /** Returns true if field readBytes is set (has been assigned a value) and false otherwise */
  public boolean isSetReadBytes() {
    return EncodingUtils.testBit(__isset_bitfield, __READBYTES_ISSET_ID);
  }

  public void setReadBytesIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __READBYTES_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NUM_ROWS:
      if (value == null) {
        unsetNumRows();
      } else {
        setNumRows((Long)value);
      }
      break;

    case NUM_BYTES:
      if (value == null) {
        unsetNumBytes();
      } else {
        setNumBytes((Long)value);
      }
      break;

    case NUM_BLOCKS:
      if (value == null) {
        unsetNumBlocks();
      } else {
        setNumBlocks((Integer)value);
      }
      break;

    case NUM_SHUFFLE_OUTPUTS:
      if (value == null) {
        unsetNumShuffleOutputs();
      } else {
        setNumShuffleOutputs((Integer)value);
      }
      break;

    case AVG_ROWS:
      if (value == null) {
        unsetAvgRows();
      } else {
        setAvgRows((Long)value);
      }
      break;

    case READ_BYTES:
      if (value == null) {
        unsetReadBytes();
      } else {
        setReadBytes((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NUM_ROWS:
      return Long.valueOf(getNumRows());

    case NUM_BYTES:
      return Long.valueOf(getNumBytes());

    case NUM_BLOCKS:
      return Integer.valueOf(getNumBlocks());

    case NUM_SHUFFLE_OUTPUTS:
      return Integer.valueOf(getNumShuffleOutputs());

    case AVG_ROWS:
      return Long.valueOf(getAvgRows());

    case READ_BYTES:
      return Long.valueOf(getReadBytes());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NUM_ROWS:
      return isSetNumRows();
    case NUM_BYTES:
      return isSetNumBytes();
    case NUM_BLOCKS:
      return isSetNumBlocks();
    case NUM_SHUFFLE_OUTPUTS:
      return isSetNumShuffleOutputs();
    case AVG_ROWS:
      return isSetAvgRows();
    case READ_BYTES:
      return isSetReadBytes();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TTableStats)
      return this.equals((TTableStats)that);
    return false;
  }

  public boolean equals(TTableStats that) {
    if (that == null)
      return false;

    boolean this_present_numRows = true;
    boolean that_present_numRows = true;
    if (this_present_numRows || that_present_numRows) {
      if (!(this_present_numRows && that_present_numRows))
        return false;
      if (this.numRows != that.numRows)
        return false;
    }

    boolean this_present_numBytes = true;
    boolean that_present_numBytes = true;
    if (this_present_numBytes || that_present_numBytes) {
      if (!(this_present_numBytes && that_present_numBytes))
        return false;
      if (this.numBytes != that.numBytes)
        return false;
    }

    boolean this_present_numBlocks = true;
    boolean that_present_numBlocks = true;
    if (this_present_numBlocks || that_present_numBlocks) {
      if (!(this_present_numBlocks && that_present_numBlocks))
        return false;
      if (this.numBlocks != that.numBlocks)
        return false;
    }

    boolean this_present_numShuffleOutputs = true;
    boolean that_present_numShuffleOutputs = true;
    if (this_present_numShuffleOutputs || that_present_numShuffleOutputs) {
      if (!(this_present_numShuffleOutputs && that_present_numShuffleOutputs))
        return false;
      if (this.numShuffleOutputs != that.numShuffleOutputs)
        return false;
    }

    boolean this_present_avgRows = true;
    boolean that_present_avgRows = true;
    if (this_present_avgRows || that_present_avgRows) {
      if (!(this_present_avgRows && that_present_avgRows))
        return false;
      if (this.avgRows != that.avgRows)
        return false;
    }

    boolean this_present_readBytes = true;
    boolean that_present_readBytes = true;
    if (this_present_readBytes || that_present_readBytes) {
      if (!(this_present_readBytes && that_present_readBytes))
        return false;
      if (this.readBytes != that.readBytes)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_numRows = true;
    list.add(present_numRows);
    if (present_numRows)
      list.add(numRows);

    boolean present_numBytes = true;
    list.add(present_numBytes);
    if (present_numBytes)
      list.add(numBytes);

    boolean present_numBlocks = true;
    list.add(present_numBlocks);
    if (present_numBlocks)
      list.add(numBlocks);

    boolean present_numShuffleOutputs = true;
    list.add(present_numShuffleOutputs);
    if (present_numShuffleOutputs)
      list.add(numShuffleOutputs);

    boolean present_avgRows = true;
    list.add(present_avgRows);
    if (present_avgRows)
      list.add(avgRows);

    boolean present_readBytes = true;
    list.add(present_readBytes);
    if (present_readBytes)
      list.add(readBytes);

    return list.hashCode();
  }

  @Override
  public int compareTo(TTableStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetNumRows()).compareTo(other.isSetNumRows());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumRows()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.numRows, other.numRows);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNumBytes()).compareTo(other.isSetNumBytes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumBytes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.numBytes, other.numBytes);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNumBlocks()).compareTo(other.isSetNumBlocks());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumBlocks()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.numBlocks, other.numBlocks);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNumShuffleOutputs()).compareTo(other.isSetNumShuffleOutputs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumShuffleOutputs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.numShuffleOutputs, other.numShuffleOutputs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAvgRows()).compareTo(other.isSetAvgRows());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAvgRows()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.avgRows, other.avgRows);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetReadBytes()).compareTo(other.isSetReadBytes());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetReadBytes()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.readBytes, other.readBytes);
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
    StringBuilder sb = new StringBuilder("TTableStats(");
    boolean first = true;

    sb.append("numRows:");
    sb.append(this.numRows);
    first = false;
    if (!first) sb.append(", ");
    sb.append("numBytes:");
    sb.append(this.numBytes);
    first = false;
    if (!first) sb.append(", ");
    sb.append("numBlocks:");
    sb.append(this.numBlocks);
    first = false;
    if (!first) sb.append(", ");
    sb.append("numShuffleOutputs:");
    sb.append(this.numShuffleOutputs);
    first = false;
    if (!first) sb.append(", ");
    sb.append("avgRows:");
    sb.append(this.avgRows);
    first = false;
    if (!first) sb.append(", ");
    sb.append("readBytes:");
    sb.append(this.readBytes);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TTableStatsStandardSchemeFactory implements SchemeFactory {
    public TTableStatsStandardScheme getScheme() {
      return new TTableStatsStandardScheme();
    }
  }

  private static class TTableStatsStandardScheme extends StandardScheme<TTableStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TTableStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NUM_ROWS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.numRows = iprot.readI64();
              struct.setNumRowsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // NUM_BYTES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.numBytes = iprot.readI64();
              struct.setNumBytesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // NUM_BLOCKS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.numBlocks = iprot.readI32();
              struct.setNumBlocksIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // NUM_SHUFFLE_OUTPUTS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.numShuffleOutputs = iprot.readI32();
              struct.setNumShuffleOutputsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // AVG_ROWS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.avgRows = iprot.readI64();
              struct.setAvgRowsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // READ_BYTES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.readBytes = iprot.readI64();
              struct.setReadBytesIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TTableStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(NUM_ROWS_FIELD_DESC);
      oprot.writeI64(struct.numRows);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_BYTES_FIELD_DESC);
      oprot.writeI64(struct.numBytes);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_BLOCKS_FIELD_DESC);
      oprot.writeI32(struct.numBlocks);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_SHUFFLE_OUTPUTS_FIELD_DESC);
      oprot.writeI32(struct.numShuffleOutputs);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(AVG_ROWS_FIELD_DESC);
      oprot.writeI64(struct.avgRows);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(READ_BYTES_FIELD_DESC);
      oprot.writeI64(struct.readBytes);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TTableStatsTupleSchemeFactory implements SchemeFactory {
    public TTableStatsTupleScheme getScheme() {
      return new TTableStatsTupleScheme();
    }
  }

  private static class TTableStatsTupleScheme extends TupleScheme<TTableStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TTableStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetNumRows()) {
        optionals.set(0);
      }
      if (struct.isSetNumBytes()) {
        optionals.set(1);
      }
      if (struct.isSetNumBlocks()) {
        optionals.set(2);
      }
      if (struct.isSetNumShuffleOutputs()) {
        optionals.set(3);
      }
      if (struct.isSetAvgRows()) {
        optionals.set(4);
      }
      if (struct.isSetReadBytes()) {
        optionals.set(5);
      }
      oprot.writeBitSet(optionals, 6);
      if (struct.isSetNumRows()) {
        oprot.writeI64(struct.numRows);
      }
      if (struct.isSetNumBytes()) {
        oprot.writeI64(struct.numBytes);
      }
      if (struct.isSetNumBlocks()) {
        oprot.writeI32(struct.numBlocks);
      }
      if (struct.isSetNumShuffleOutputs()) {
        oprot.writeI32(struct.numShuffleOutputs);
      }
      if (struct.isSetAvgRows()) {
        oprot.writeI64(struct.avgRows);
      }
      if (struct.isSetReadBytes()) {
        oprot.writeI64(struct.readBytes);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TTableStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(6);
      if (incoming.get(0)) {
        struct.numRows = iprot.readI64();
        struct.setNumRowsIsSet(true);
      }
      if (incoming.get(1)) {
        struct.numBytes = iprot.readI64();
        struct.setNumBytesIsSet(true);
      }
      if (incoming.get(2)) {
        struct.numBlocks = iprot.readI32();
        struct.setNumBlocksIsSet(true);
      }
      if (incoming.get(3)) {
        struct.numShuffleOutputs = iprot.readI32();
        struct.setNumShuffleOutputsIsSet(true);
      }
      if (incoming.get(4)) {
        struct.avgRows = iprot.readI64();
        struct.setAvgRowsIsSet(true);
      }
      if (incoming.get(5)) {
        struct.readBytes = iprot.readI64();
        struct.setReadBytesIsSet(true);
      }
    }
  }

}

