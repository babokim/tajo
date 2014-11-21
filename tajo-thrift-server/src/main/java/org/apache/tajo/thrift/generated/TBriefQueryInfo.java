/**
 * Autogenerated by Thrift Compiler (0.9.1)
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TBriefQueryInfo implements org.apache.thrift.TBase<TBriefQueryInfo, TBriefQueryInfo._Fields>, java.io.Serializable, Cloneable, Comparable<TBriefQueryInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TBriefQueryInfo");

  private static final org.apache.thrift.protocol.TField QUERY_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("queryId", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField STATE_FIELD_DESC = new org.apache.thrift.protocol.TField("state", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField START_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("startTime", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField FINISH_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("finishTime", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField QUERY_FIELD_DESC = new org.apache.thrift.protocol.TField("query", org.apache.thrift.protocol.TType.STRING, (short)5);
  private static final org.apache.thrift.protocol.TField QUERY_MASTER_HOST_FIELD_DESC = new org.apache.thrift.protocol.TField("queryMasterHost", org.apache.thrift.protocol.TType.STRING, (short)6);
  private static final org.apache.thrift.protocol.TField QUERY_MASTER_PORT_FIELD_DESC = new org.apache.thrift.protocol.TField("queryMasterPort", org.apache.thrift.protocol.TType.I32, (short)7);
  private static final org.apache.thrift.protocol.TField PROGRESS_FIELD_DESC = new org.apache.thrift.protocol.TField("progress", org.apache.thrift.protocol.TType.DOUBLE, (short)8);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TBriefQueryInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TBriefQueryInfoTupleSchemeFactory());
  }

  public String queryId; // required
  public String state; // required
  public long startTime; // required
  public long finishTime; // required
  public String query; // required
  public String queryMasterHost; // required
  public int queryMasterPort; // required
  public double progress; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QUERY_ID((short)1, "queryId"),
    STATE((short)2, "state"),
    START_TIME((short)3, "startTime"),
    FINISH_TIME((short)4, "finishTime"),
    QUERY((short)5, "query"),
    QUERY_MASTER_HOST((short)6, "queryMasterHost"),
    QUERY_MASTER_PORT((short)7, "queryMasterPort"),
    PROGRESS((short)8, "progress");

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
        case 1: // QUERY_ID
          return QUERY_ID;
        case 2: // STATE
          return STATE;
        case 3: // START_TIME
          return START_TIME;
        case 4: // FINISH_TIME
          return FINISH_TIME;
        case 5: // QUERY
          return QUERY;
        case 6: // QUERY_MASTER_HOST
          return QUERY_MASTER_HOST;
        case 7: // QUERY_MASTER_PORT
          return QUERY_MASTER_PORT;
        case 8: // PROGRESS
          return PROGRESS;
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
  private static final int __STARTTIME_ISSET_ID = 0;
  private static final int __FINISHTIME_ISSET_ID = 1;
  private static final int __QUERYMASTERPORT_ISSET_ID = 2;
  private static final int __PROGRESS_ISSET_ID = 3;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.QUERY_ID, new org.apache.thrift.meta_data.FieldMetaData("queryId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STATE, new org.apache.thrift.meta_data.FieldMetaData("state", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.START_TIME, new org.apache.thrift.meta_data.FieldMetaData("startTime", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.FINISH_TIME, new org.apache.thrift.meta_data.FieldMetaData("finishTime", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.QUERY, new org.apache.thrift.meta_data.FieldMetaData("query", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.QUERY_MASTER_HOST, new org.apache.thrift.meta_data.FieldMetaData("queryMasterHost", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.QUERY_MASTER_PORT, new org.apache.thrift.meta_data.FieldMetaData("queryMasterPort", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.PROGRESS, new org.apache.thrift.meta_data.FieldMetaData("progress", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TBriefQueryInfo.class, metaDataMap);
  }

  public TBriefQueryInfo() {
  }

  public TBriefQueryInfo(
    String queryId,
    String state,
    long startTime,
    long finishTime,
    String query,
    String queryMasterHost,
    int queryMasterPort,
    double progress)
  {
    this();
    this.queryId = queryId;
    this.state = state;
    this.startTime = startTime;
    setStartTimeIsSet(true);
    this.finishTime = finishTime;
    setFinishTimeIsSet(true);
    this.query = query;
    this.queryMasterHost = queryMasterHost;
    this.queryMasterPort = queryMasterPort;
    setQueryMasterPortIsSet(true);
    this.progress = progress;
    setProgressIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TBriefQueryInfo(TBriefQueryInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetQueryId()) {
      this.queryId = other.queryId;
    }
    if (other.isSetState()) {
      this.state = other.state;
    }
    this.startTime = other.startTime;
    this.finishTime = other.finishTime;
    if (other.isSetQuery()) {
      this.query = other.query;
    }
    if (other.isSetQueryMasterHost()) {
      this.queryMasterHost = other.queryMasterHost;
    }
    this.queryMasterPort = other.queryMasterPort;
    this.progress = other.progress;
  }

  public TBriefQueryInfo deepCopy() {
    return new TBriefQueryInfo(this);
  }

  @Override
  public void clear() {
    this.queryId = null;
    this.state = null;
    setStartTimeIsSet(false);
    this.startTime = 0;
    setFinishTimeIsSet(false);
    this.finishTime = 0;
    this.query = null;
    this.queryMasterHost = null;
    setQueryMasterPortIsSet(false);
    this.queryMasterPort = 0;
    setProgressIsSet(false);
    this.progress = 0.0;
  }

  public String getQueryId() {
    return this.queryId;
  }

  public TBriefQueryInfo setQueryId(String queryId) {
    this.queryId = queryId;
    return this;
  }

  public void unsetQueryId() {
    this.queryId = null;
  }

  /** Returns true if field queryId is set (has been assigned a value) and false otherwise */
  public boolean isSetQueryId() {
    return this.queryId != null;
  }

  public void setQueryIdIsSet(boolean value) {
    if (!value) {
      this.queryId = null;
    }
  }

  public String getState() {
    return this.state;
  }

  public TBriefQueryInfo setState(String state) {
    this.state = state;
    return this;
  }

  public void unsetState() {
    this.state = null;
  }

  /** Returns true if field state is set (has been assigned a value) and false otherwise */
  public boolean isSetState() {
    return this.state != null;
  }

  public void setStateIsSet(boolean value) {
    if (!value) {
      this.state = null;
    }
  }

  public long getStartTime() {
    return this.startTime;
  }

  public TBriefQueryInfo setStartTime(long startTime) {
    this.startTime = startTime;
    setStartTimeIsSet(true);
    return this;
  }

  public void unsetStartTime() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __STARTTIME_ISSET_ID);
  }

  /** Returns true if field startTime is set (has been assigned a value) and false otherwise */
  public boolean isSetStartTime() {
    return EncodingUtils.testBit(__isset_bitfield, __STARTTIME_ISSET_ID);
  }

  public void setStartTimeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __STARTTIME_ISSET_ID, value);
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public TBriefQueryInfo setFinishTime(long finishTime) {
    this.finishTime = finishTime;
    setFinishTimeIsSet(true);
    return this;
  }

  public void unsetFinishTime() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __FINISHTIME_ISSET_ID);
  }

  /** Returns true if field finishTime is set (has been assigned a value) and false otherwise */
  public boolean isSetFinishTime() {
    return EncodingUtils.testBit(__isset_bitfield, __FINISHTIME_ISSET_ID);
  }

  public void setFinishTimeIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __FINISHTIME_ISSET_ID, value);
  }

  public String getQuery() {
    return this.query;
  }

  public TBriefQueryInfo setQuery(String query) {
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

  public String getQueryMasterHost() {
    return this.queryMasterHost;
  }

  public TBriefQueryInfo setQueryMasterHost(String queryMasterHost) {
    this.queryMasterHost = queryMasterHost;
    return this;
  }

  public void unsetQueryMasterHost() {
    this.queryMasterHost = null;
  }

  /** Returns true if field queryMasterHost is set (has been assigned a value) and false otherwise */
  public boolean isSetQueryMasterHost() {
    return this.queryMasterHost != null;
  }

  public void setQueryMasterHostIsSet(boolean value) {
    if (!value) {
      this.queryMasterHost = null;
    }
  }

  public int getQueryMasterPort() {
    return this.queryMasterPort;
  }

  public TBriefQueryInfo setQueryMasterPort(int queryMasterPort) {
    this.queryMasterPort = queryMasterPort;
    setQueryMasterPortIsSet(true);
    return this;
  }

  public void unsetQueryMasterPort() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __QUERYMASTERPORT_ISSET_ID);
  }

  /** Returns true if field queryMasterPort is set (has been assigned a value) and false otherwise */
  public boolean isSetQueryMasterPort() {
    return EncodingUtils.testBit(__isset_bitfield, __QUERYMASTERPORT_ISSET_ID);
  }

  public void setQueryMasterPortIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __QUERYMASTERPORT_ISSET_ID, value);
  }

  public double getProgress() {
    return this.progress;
  }

  public TBriefQueryInfo setProgress(double progress) {
    this.progress = progress;
    setProgressIsSet(true);
    return this;
  }

  public void unsetProgress() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PROGRESS_ISSET_ID);
  }

  /** Returns true if field progress is set (has been assigned a value) and false otherwise */
  public boolean isSetProgress() {
    return EncodingUtils.testBit(__isset_bitfield, __PROGRESS_ISSET_ID);
  }

  public void setProgressIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PROGRESS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case QUERY_ID:
      if (value == null) {
        unsetQueryId();
      } else {
        setQueryId((String)value);
      }
      break;

    case STATE:
      if (value == null) {
        unsetState();
      } else {
        setState((String)value);
      }
      break;

    case START_TIME:
      if (value == null) {
        unsetStartTime();
      } else {
        setStartTime((Long)value);
      }
      break;

    case FINISH_TIME:
      if (value == null) {
        unsetFinishTime();
      } else {
        setFinishTime((Long)value);
      }
      break;

    case QUERY:
      if (value == null) {
        unsetQuery();
      } else {
        setQuery((String)value);
      }
      break;

    case QUERY_MASTER_HOST:
      if (value == null) {
        unsetQueryMasterHost();
      } else {
        setQueryMasterHost((String)value);
      }
      break;

    case QUERY_MASTER_PORT:
      if (value == null) {
        unsetQueryMasterPort();
      } else {
        setQueryMasterPort((Integer)value);
      }
      break;

    case PROGRESS:
      if (value == null) {
        unsetProgress();
      } else {
        setProgress((Double)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case QUERY_ID:
      return getQueryId();

    case STATE:
      return getState();

    case START_TIME:
      return Long.valueOf(getStartTime());

    case FINISH_TIME:
      return Long.valueOf(getFinishTime());

    case QUERY:
      return getQuery();

    case QUERY_MASTER_HOST:
      return getQueryMasterHost();

    case QUERY_MASTER_PORT:
      return Integer.valueOf(getQueryMasterPort());

    case PROGRESS:
      return Double.valueOf(getProgress());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case QUERY_ID:
      return isSetQueryId();
    case STATE:
      return isSetState();
    case START_TIME:
      return isSetStartTime();
    case FINISH_TIME:
      return isSetFinishTime();
    case QUERY:
      return isSetQuery();
    case QUERY_MASTER_HOST:
      return isSetQueryMasterHost();
    case QUERY_MASTER_PORT:
      return isSetQueryMasterPort();
    case PROGRESS:
      return isSetProgress();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TBriefQueryInfo)
      return this.equals((TBriefQueryInfo)that);
    return false;
  }

  public boolean equals(TBriefQueryInfo that) {
    if (that == null)
      return false;

    boolean this_present_queryId = true && this.isSetQueryId();
    boolean that_present_queryId = true && that.isSetQueryId();
    if (this_present_queryId || that_present_queryId) {
      if (!(this_present_queryId && that_present_queryId))
        return false;
      if (!this.queryId.equals(that.queryId))
        return false;
    }

    boolean this_present_state = true && this.isSetState();
    boolean that_present_state = true && that.isSetState();
    if (this_present_state || that_present_state) {
      if (!(this_present_state && that_present_state))
        return false;
      if (!this.state.equals(that.state))
        return false;
    }

    boolean this_present_startTime = true;
    boolean that_present_startTime = true;
    if (this_present_startTime || that_present_startTime) {
      if (!(this_present_startTime && that_present_startTime))
        return false;
      if (this.startTime != that.startTime)
        return false;
    }

    boolean this_present_finishTime = true;
    boolean that_present_finishTime = true;
    if (this_present_finishTime || that_present_finishTime) {
      if (!(this_present_finishTime && that_present_finishTime))
        return false;
      if (this.finishTime != that.finishTime)
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

    boolean this_present_queryMasterHost = true && this.isSetQueryMasterHost();
    boolean that_present_queryMasterHost = true && that.isSetQueryMasterHost();
    if (this_present_queryMasterHost || that_present_queryMasterHost) {
      if (!(this_present_queryMasterHost && that_present_queryMasterHost))
        return false;
      if (!this.queryMasterHost.equals(that.queryMasterHost))
        return false;
    }

    boolean this_present_queryMasterPort = true;
    boolean that_present_queryMasterPort = true;
    if (this_present_queryMasterPort || that_present_queryMasterPort) {
      if (!(this_present_queryMasterPort && that_present_queryMasterPort))
        return false;
      if (this.queryMasterPort != that.queryMasterPort)
        return false;
    }

    boolean this_present_progress = true;
    boolean that_present_progress = true;
    if (this_present_progress || that_present_progress) {
      if (!(this_present_progress && that_present_progress))
        return false;
      if (this.progress != that.progress)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TBriefQueryInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetQueryId()).compareTo(other.isSetQueryId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryId, other.queryId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetState()).compareTo(other.isSetState());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetState()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.state, other.state);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStartTime()).compareTo(other.isSetStartTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.startTime, other.startTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFinishTime()).compareTo(other.isSetFinishTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFinishTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.finishTime, other.finishTime);
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
    lastComparison = Boolean.valueOf(isSetQueryMasterHost()).compareTo(other.isSetQueryMasterHost());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryMasterHost()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryMasterHost, other.queryMasterHost);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetQueryMasterPort()).compareTo(other.isSetQueryMasterPort());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryMasterPort()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryMasterPort, other.queryMasterPort);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetProgress()).compareTo(other.isSetProgress());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProgress()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.progress, other.progress);
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
    StringBuilder sb = new StringBuilder("TBriefQueryInfo(");
    boolean first = true;

    sb.append("queryId:");
    if (this.queryId == null) {
      sb.append("null");
    } else {
      sb.append(this.queryId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("state:");
    if (this.state == null) {
      sb.append("null");
    } else {
      sb.append(this.state);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("startTime:");
    sb.append(this.startTime);
    first = false;
    if (!first) sb.append(", ");
    sb.append("finishTime:");
    sb.append(this.finishTime);
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
    sb.append("queryMasterHost:");
    if (this.queryMasterHost == null) {
      sb.append("null");
    } else {
      sb.append(this.queryMasterHost);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("queryMasterPort:");
    sb.append(this.queryMasterPort);
    first = false;
    if (!first) sb.append(", ");
    sb.append("progress:");
    sb.append(this.progress);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
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

  private static class TBriefQueryInfoStandardSchemeFactory implements SchemeFactory {
    public TBriefQueryInfoStandardScheme getScheme() {
      return new TBriefQueryInfoStandardScheme();
    }
  }

  private static class TBriefQueryInfoStandardScheme extends StandardScheme<TBriefQueryInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TBriefQueryInfo struct) throws TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // QUERY_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.queryId = iprot.readString();
              struct.setQueryIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STATE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.state = iprot.readString();
              struct.setStateIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // START_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.startTime = iprot.readI64();
              struct.setStartTimeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // FINISH_TIME
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.finishTime = iprot.readI64();
              struct.setFinishTimeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // QUERY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.query = iprot.readString();
              struct.setQueryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // QUERY_MASTER_HOST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.queryMasterHost = iprot.readString();
              struct.setQueryMasterHostIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // QUERY_MASTER_PORT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.queryMasterPort = iprot.readI32();
              struct.setQueryMasterPortIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 8: // PROGRESS
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.progress = iprot.readDouble();
              struct.setProgressIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TBriefQueryInfo struct) throws TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.queryId != null) {
        oprot.writeFieldBegin(QUERY_ID_FIELD_DESC);
        oprot.writeString(struct.queryId);
        oprot.writeFieldEnd();
      }
      if (struct.state != null) {
        oprot.writeFieldBegin(STATE_FIELD_DESC);
        oprot.writeString(struct.state);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(START_TIME_FIELD_DESC);
      oprot.writeI64(struct.startTime);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(FINISH_TIME_FIELD_DESC);
      oprot.writeI64(struct.finishTime);
      oprot.writeFieldEnd();
      if (struct.query != null) {
        oprot.writeFieldBegin(QUERY_FIELD_DESC);
        oprot.writeString(struct.query);
        oprot.writeFieldEnd();
      }
      if (struct.queryMasterHost != null) {
        oprot.writeFieldBegin(QUERY_MASTER_HOST_FIELD_DESC);
        oprot.writeString(struct.queryMasterHost);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(QUERY_MASTER_PORT_FIELD_DESC);
      oprot.writeI32(struct.queryMasterPort);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(PROGRESS_FIELD_DESC);
      oprot.writeDouble(struct.progress);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TBriefQueryInfoTupleSchemeFactory implements SchemeFactory {
    public TBriefQueryInfoTupleScheme getScheme() {
      return new TBriefQueryInfoTupleScheme();
    }
  }

  private static class TBriefQueryInfoTupleScheme extends TupleScheme<TBriefQueryInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TBriefQueryInfo struct) throws TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetQueryId()) {
        optionals.set(0);
      }
      if (struct.isSetState()) {
        optionals.set(1);
      }
      if (struct.isSetStartTime()) {
        optionals.set(2);
      }
      if (struct.isSetFinishTime()) {
        optionals.set(3);
      }
      if (struct.isSetQuery()) {
        optionals.set(4);
      }
      if (struct.isSetQueryMasterHost()) {
        optionals.set(5);
      }
      if (struct.isSetQueryMasterPort()) {
        optionals.set(6);
      }
      if (struct.isSetProgress()) {
        optionals.set(7);
      }
      oprot.writeBitSet(optionals, 8);
      if (struct.isSetQueryId()) {
        oprot.writeString(struct.queryId);
      }
      if (struct.isSetState()) {
        oprot.writeString(struct.state);
      }
      if (struct.isSetStartTime()) {
        oprot.writeI64(struct.startTime);
      }
      if (struct.isSetFinishTime()) {
        oprot.writeI64(struct.finishTime);
      }
      if (struct.isSetQuery()) {
        oprot.writeString(struct.query);
      }
      if (struct.isSetQueryMasterHost()) {
        oprot.writeString(struct.queryMasterHost);
      }
      if (struct.isSetQueryMasterPort()) {
        oprot.writeI32(struct.queryMasterPort);
      }
      if (struct.isSetProgress()) {
        oprot.writeDouble(struct.progress);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TBriefQueryInfo struct) throws TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(8);
      if (incoming.get(0)) {
        struct.queryId = iprot.readString();
        struct.setQueryIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.state = iprot.readString();
        struct.setStateIsSet(true);
      }
      if (incoming.get(2)) {
        struct.startTime = iprot.readI64();
        struct.setStartTimeIsSet(true);
      }
      if (incoming.get(3)) {
        struct.finishTime = iprot.readI64();
        struct.setFinishTimeIsSet(true);
      }
      if (incoming.get(4)) {
        struct.query = iprot.readString();
        struct.setQueryIsSet(true);
      }
      if (incoming.get(5)) {
        struct.queryMasterHost = iprot.readString();
        struct.setQueryMasterHostIsSet(true);
      }
      if (incoming.get(6)) {
        struct.queryMasterPort = iprot.readI32();
        struct.setQueryMasterPortIsSet(true);
      }
      if (incoming.get(7)) {
        struct.progress = iprot.readDouble();
        struct.setProgressIsSet(true);
      }
    }
  }

}

