/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.hbase;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.SerializerDeserializer;
import org.apache.tajo.util.Bytes;

import java.io.IOException;
import java.io.OutputStream;

public class HBaseBinarySerializerDeserializer implements SerializerDeserializer {
  @Override
  public int serialize(Column col, Datum datum, OutputStream out, byte[] nullCharacters) throws IOException {
    return 0;
  }

  @Override
  public Datum deserialize(Column col, byte[] bytes, int offset, int length, byte[] nullCharacters) throws IOException {
    Datum datum;
    switch (col.getDataType().getType()) {
      case INT1:
      case INT2:
        datum = bytes == null ? NullDatum.get() : DatumFactory.createInt2(Bytes.toShort(bytes, offset, length));
        break;
      case INT4:
        datum = bytes == null ? NullDatum.get() : DatumFactory.createInt4(Bytes.toInt(bytes, offset, length));
        break;
      case INT8:
        if (length == 4) {
          datum = bytes == null ? NullDatum.get() : DatumFactory.createInt8(Bytes.toInt(bytes, offset, length));
        } else {
          datum = bytes == null ? NullDatum.get() : DatumFactory.createInt8(Bytes.toLong(bytes, offset, length));
        }
        break;
      case FLOAT4:
        datum = bytes == null ? NullDatum.get() : DatumFactory.createFloat4(Bytes.toFloat(bytes, offset));
        break;
      case FLOAT8:
        datum = bytes == null ? NullDatum.get() : DatumFactory.createFloat8(Bytes.toDouble(bytes, offset));
        break;
      case TEXT:
        datum = bytes == null ? NullDatum.get() : DatumFactory.createText(bytes);
        break;
      default:
        datum = NullDatum.get();
        break;
    }
    return datum;
  }
}
