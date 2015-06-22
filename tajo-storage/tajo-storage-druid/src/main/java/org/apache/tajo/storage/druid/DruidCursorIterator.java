/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.druid;

import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DruidCursorIterator implements Iterator<Tuple> {
  private Cursor cursor;
  private List<String> dims;
  private List<String> metrics;
  private int timestampColumnIndex;
  private int numTargetColumns;
  private Map<String, Integer> targetIndexesMap;
  private int[] targetIndexes;

  private LongColumnSelector timestampColumnSelector;
  private DimensionSelector[] dimSelectors;
  private ObjectColumnSelector[] metSelectors;

  public DruidCursorIterator(Map<String, Integer> targetIndexesMap, List<String> dims, List<String> metrics,
                             int timestampColumnIndex) {
    this.dims = dims;
    this.metrics = metrics;

    this.timestampColumnIndex = timestampColumnIndex;
    this.numTargetColumns = targetIndexesMap.size();
    this.targetIndexesMap = targetIndexesMap;
  }

  public void setCursor(Cursor cursor) {
    this.cursor = cursor;
    timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);
    List<DimensionSelector> dimSelectorList = new ArrayList<DimensionSelector>();
    targetIndexes = new int[numTargetColumns];
    int tIndex = 0;

    for (String dim : dims) {
      if (targetIndexesMap.containsKey(dim)) {
        if (timestampColumnIndex >= 0 && tIndex == timestampColumnIndex) {
          targetIndexes[tIndex++] = -1;
        }
        final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim, null);
        dimSelectorList.add(dimSelector);
        targetIndexes[tIndex++] = targetIndexesMap.get(dim);
      }
    }

    dimSelectors = dimSelectorList.toArray(new DimensionSelector[]{});

    List<ObjectColumnSelector> metSelectorList = new ArrayList<ObjectColumnSelector>();
    for (String metric : metrics) {
      if (targetIndexesMap.containsKey(metric)) {
        if (timestampColumnIndex >= 0 && tIndex == timestampColumnIndex) {
          targetIndexes[tIndex++] = -1;
        }
        final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
        metSelectorList.add(metricSelector);
        targetIndexes[tIndex++] = targetIndexesMap.get(metric);
      }
    }
    metSelectors = metSelectorList.toArray(new ObjectColumnSelector[]{});
  }

  @Override
  public boolean hasNext() {
    return cursor != null && !cursor.isDone();
  }

  @Override
  public Tuple next() {
    if (cursor == null || cursor.isDone()) {
      return null;
    }

    Tuple tuple = new VTuple(numTargetColumns);

    if (timestampColumnIndex >= 0) {
      tuple.put(timestampColumnIndex, new Int8Datum(timestampColumnSelector.get()));
    }

    int index = 0;
    for (DimensionSelector dimSelector: dimSelectors) {
      if (timestampColumnIndex >= 0 && index == timestampColumnIndex) {
        index++;
      }
      final IndexedInts vals = dimSelector.getRow();

      String dimValue = "";
      if (vals.size() == 1) {
        dimValue = dimSelector.lookupName(vals.get(0));
      } else {
        String delim = "";
        for (int i = 0; i < vals.size(); ++i) {
          dimValue += delim + dimSelector.lookupName(vals.get(i));
          delim = ",";
        }
      }
      tuple.put(targetIndexes[index++], new TextDatum(dimValue));
    }

    for (ObjectColumnSelector metSelector: metSelectors) {
      if (timestampColumnIndex >= 0 && index == timestampColumnIndex) {
        index++;
      }
      tuple.put(targetIndexes[index++], makeMetricsDatum(metSelector.get()));
    }

    cursor.advance();

    return tuple;
  }

  @Override
  public void remove() {
  }

  private Datum makeMetricsDatum(Object obj) {
    if (obj instanceof Integer) {
      return new Int4Datum((Integer)obj);
    } else if (obj instanceof Long) {
      return new Int8Datum((Long)obj);
    } else if (obj instanceof Float) {
      return new Float4Datum((Float)obj);
    } else if (obj instanceof Double) {
      return new Float8Datum((Double)obj);
    } else {
      return new TextDatum(obj.toString());
    }
  }
}
