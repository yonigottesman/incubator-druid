/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.incremental;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.ColumnValueSelector;
import com.oath.oak.OakMap;
import com.oath.oak.OakMapBuilder;
import com.oath.oak.OakTransformView;
import com.oath.oak.OakBufferView;
import com.oath.oak.OakRBuffer;
import com.oath.oak.OakCloseableIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import sun.misc.VM;

import javax.annotation.Nullable;


/**
 */
public class OakIncrementalIndex extends InternalDataIncrementalIndex<BufferAggregator>
{

  // When serializing an object from IncrementalIndexRow.dims, we use:
  // 1. 4 bytes for representing its type (Double, Float, Long or String)
  // 2. 8 bytes for saving its value or the array position and length (in the case of String)
  static final Integer ALLOC_PER_DIM = 12;
  static final Integer NO_DIM = -1;
  static final Integer TIME_STAMP_INDEX = 0;
  static final Integer DIMS_LENGTH_INDEX = TIME_STAMP_INDEX + Long.BYTES;
  static final Integer ROW_INDEX_INDEX = DIMS_LENGTH_INDEX + Integer.BYTES;
  static final Integer DIMS_INDEX = ROW_INDEX_INDEX + Integer.BYTES;
  // Serialization and deserialization offsets
  static final Integer VALUE_TYPE_OFFSET = 0;
  static final Integer DATA_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_INDEX_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_LENGTH_OFFSET = ARRAY_INDEX_OFFSET + Integer.BYTES;

  OakMap<IncrementalIndexRow, Row> oak;

  private OffheapAggsManager aggsManager;
  private AtomicInteger rowIndexGenerator; // Only used in Plain mode

  Map<String, String> env = System.getenv();

  OakIncrementalIndex(
          IncrementalIndexSchema incrementalIndexSchema,
          boolean deserializeComplexMetrics,
          boolean reportParseExceptions,
          boolean concurrentEventAdd,
          int maxRowCount
  )
  {
    super(incrementalIndexSchema, reportParseExceptions, maxRowCount);

    this.aggsManager = new OffheapAggsManager(incrementalIndexSchema, deserializeComplexMetrics,
            concurrentEventAdd, rowSupplier,
            columnCapabilities, null, this);

    this.rowIndexGenerator = new AtomicInteger(IncrementalIndexRow.EMPTY_ROW_INDEX);
    IncrementalIndexRow minIncrementalIndexRow = getMinIncrementalIndexRow();

    long maxDirectMemory = VM.maxDirectMemory();
    int memoryCapacity = Integer.MAX_VALUE;
    if (maxDirectMemory < memoryCapacity) {
      memoryCapacity = (int) maxDirectMemory;
    }

    OakMapBuilder builder = new OakMapBuilder()
            .setKeySerializer(new OakKeySerializer(dimensionDescsList))
            .setValueSerializer(new OakValueSerializer(aggsManager, reportParseExceptions, in))
            .setMinKey(minIncrementalIndexRow)
            .setComparator(new OakKeysComparator(dimensionDescsList, this.isRollup()))
            .setMemoryCapacity(memoryCapacity);

    if (env != null) {
      String chunkMaxItems = env.get("chunkMaxItems");
      if (chunkMaxItems != null) {
        builder = builder.setChunkMaxItems(Integer.getInteger(chunkMaxItems));
      }
      String chunkBytesPerItem = env.get("chunkBytesPerItem");
      if (chunkMaxItems != null) {
        builder = builder.setChunkBytesPerItem(Integer.getInteger(chunkBytesPerItem));
      }
    }

    oak = builder.build();
  }

  @Override
  public int getLastRowIndex()
  {
    return this.rowIndexGenerator.get();
  }

  public Granularity getGranularity()
  {
    return gran;
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(List<PostAggregator> postAggs, boolean descending)
  {
    Function<Map.Entry<ByteBuffer, ByteBuffer>, Row> transformer = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Row>()
    {

      @Override
      public Row apply(Map.Entry<ByteBuffer, ByteBuffer> entry)
      {
        ByteBuffer serializedKey = entry.getKey();
        ByteBuffer serializedValue = entry.getValue();
        long timeStamp = OakIncrementalIndex.getTimestamp(serializedKey);
        int dimsLength = OakIncrementalIndex.getDimsLength(serializedKey);
        Map<String, Object> theVals = Maps.newLinkedHashMap();
        for (int i = 0; i < dimsLength; ++i) {
          Object dim = OakIncrementalIndex.getDimValue(serializedKey, i);
          DimensionDesc dimensionDesc = dimensionDescsList.get(i);
          if (dimensionDesc == null) {
            continue;
          }
          String dimensionName = dimensionDesc.getName();
          DimensionHandler handler = dimensionDesc.getHandler();
          if (dim == null || handler.getLengthOfEncodedKeyComponent(dim) == 0) {
            theVals.put(dimensionName, null);
            continue;
          }
          final DimensionIndexer indexer = dimensionDesc.getIndexer();
          Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualList(dim);
          theVals.put(dimensionName, rowVals);
        }

        BufferAggregator[] aggs = aggsManager.getAggs();
        for (int i = 0; i < aggs.length; ++i) {
          theVals.put(aggsManager.metrics[i].getName(), aggs[i].get(serializedValue, aggsManager.aggOffsetInBuffer[i]));
        }

        return new MapBasedRow(timeStamp, theVals);
      }
    };

    OakMap tmpOakMap = descending ? oak.descendingMap() : oak;

    try (OakTransformView transformView = tmpOakMap.createTransformView(transformer)) {
      OakCloseableIterator<Row> valuesIterator = transformView.entriesIterator();
      return () -> Iterators.transform(
          valuesIterator,
          row -> row
      );
    }
  }

  @Override
  public Iterable<IncrementalIndexRow> persistIterable()
  {
    return keySet();
  }

  @Override
  public void close()
  {
    oak.close();
  }

  @Override
  protected long getMinTimeMillis()
  {
    return oak.getMinKey().getTimestamp();
  }

  @Override
  protected long getMaxTimeMillis()
  {
    return oak.getMaxKey().getTimestamp();
  }

  @Override
  protected BufferAggregator[] getAggsForRow(IncrementalIndexRow incrementalIndexRow)
  {
    return aggsManager.getAggs();
  }

  @Override
  protected Object getAggVal(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    Function<Map.Entry<ByteBuffer, ByteBuffer>, Object> transformer = entry -> {
      ByteBuffer serializedValue = entry.getValue();
      BufferAggregator agg = aggsManager.getAggs()[aggIndex];
      return agg.get(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
    };

    try (OakTransformView<IncrementalIndexRow, Object> transformView = oak.createTransformView(transformer)) {
      return transformView.get(incrementalIndexRow);
    }
  }

  @Override
  protected float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    Function<Map.Entry<ByteBuffer, ByteBuffer>, Float> transformer = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Float>() {
      @Override
      public Float apply(Map.Entry<ByteBuffer, ByteBuffer> entry)
      {
        ByteBuffer serializedValue = entry.getValue();
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.getFloat(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      }
    };

    try (OakTransformView<IncrementalIndexRow, Float> transformView = oak.createTransformView(transformer)) {
      return transformView.get(incrementalIndexRow);
    }
  }

  @Override
  protected long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    Function<Map.Entry<ByteBuffer, ByteBuffer>, Long> transformer = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Long>() {
      @Override
      public Long apply(Map.Entry<ByteBuffer, ByteBuffer> entry)
      {
        ByteBuffer serializedValue = entry.getValue();
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.getLong(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      }
    };

    try (OakTransformView<IncrementalIndexRow, Long> transformView = oak.createTransformView(transformer)) {
      return transformView.get(incrementalIndexRow);
    }
  }

  @Override
  protected Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    Function<Map.Entry<ByteBuffer, ByteBuffer>, Object> transformer = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Object>() {
      @Override
      public Object apply(Map.Entry<ByteBuffer, ByteBuffer> entry)
      {
        ByteBuffer serializedValue = entry.getValue();
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.get(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      }
    };

    try (OakTransformView<IncrementalIndexRow, Object> transformView = oak.createTransformView(transformer)) {
      return transformView.get(incrementalIndexRow);
    }
  }

  @Override
  protected double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    Function<Map.Entry<ByteBuffer, ByteBuffer>, Double> transformer = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Double>() {
      @Override
      public Double apply(Map.Entry<ByteBuffer, ByteBuffer> entry)
      {
        ByteBuffer serializedValue = entry.getValue();
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.getDouble(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      }
    };
    try (OakTransformView<IncrementalIndexRow, Double> transformView = oak.createTransformView(transformer)) {
      return transformView.get(incrementalIndexRow);
    }
  }

  @Override
  protected boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    Function<Map.Entry<ByteBuffer, ByteBuffer>, Boolean> transformer = new Function<Map.Entry<ByteBuffer, ByteBuffer>, Boolean>() {
      @Override
      public Boolean apply(Map.Entry<ByteBuffer, ByteBuffer> entry)
      {
        ByteBuffer serializedValue = entry.getValue();
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.isNull(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      }
    };

    try (OakTransformView<IncrementalIndexRow, Boolean> transformView = oak.createTransformView(transformer)) {
      return transformView.get(incrementalIndexRow);
    }
  }

  @Override
  public Iterable<IncrementalIndexRow> timeRangeIterable(
          boolean descending, long timeStart, long timeEnd)
  {
    if (timeStart > timeEnd) {
      return null;
    }

    IncrementalIndexRow from = new IncrementalIndexRow(timeStart, null, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);
    IncrementalIndexRow to = new IncrementalIndexRow(timeEnd + 1, null, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);
    try (OakMap subMap = oak.subMap(from, true, to, false, descending);
         OakBufferView bufferView = subMap.createBufferView()) {

      OakCloseableIterator<OakRBuffer> keysIterator = bufferView.keysIterator();
      return () -> Iterators.transform(
          keysIterator,
          oakRBuffer -> new OakIncrementalIndexReadOnlyRow(oakRBuffer, dimensionDescsList)
      );


    }

  }

  @Override
  public Iterable<IncrementalIndexRow> keySet()
  {
    OakCloseableIterator<IncrementalIndexRow> keysIterator = oak.keysIterator();

    return new Iterable<IncrementalIndexRow>() {
      @Override
      public Iterator<IncrementalIndexRow> iterator()
      {
        return Iterators.transform(
            keysIterator,
            key -> key
        );
      }
    };
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  private Integer addToOak(
          InputRow row,
          AtomicInteger numEntries,
          IncrementalIndexRow incrementalIndexRow,
          ThreadLocal<InputRow> rowContainer,
          boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    OakComputer computer = new OakComputer(aggsManager, reportParseExceptions,
            row, rowContainer);

    if (isRollup()) {
      incrementalIndexRow.setRowIndex(IncrementalIndexRow.EMPTY_ROW_INDEX);
    } else {
      incrementalIndexRow.setRowIndex(rowIndexGenerator.getAndIncrement());
    }

    if (numEntries.get() < maxRowCount || skipMaxRowsInMemoryCheck) {
      oak.putIfAbsentComputeIfPresent(incrementalIndexRow, row, computer);

      int currSize = oak.entries();
      int prev = numEntries.get();
      while (currSize > prev) {
        if (numEntries.compareAndSet(prev, currSize)) {
          break;
        }
        prev = numEntries.get();
      }

    } else {
      if (!oak.computeIfPresent(incrementalIndexRow, computer)) { // the key wasn't in oak
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
    }
    return numEntries.get();
  }

  @Override
  public AggregatorFactory[] getMetricAggs()
  {
    return aggsManager.getMetricAggs();
  }

  @Override
  public IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    row = formatRow(row);
    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, DateTimes.utc(minTimestamp));
    }

    OakIncrementalIndexRow oakIncrementalIndexRow = toOakIncrementalIndexRow(row);
    final int rv = addToOak(
            row,
            numEntries,
            oakIncrementalIndexRow,
            in,
            skipMaxRowsInMemoryCheck
    );
    updateMaxIngestedTime(row.getTimestamp());
    return new IncrementalIndexAddResult(rv, 0, null);
  }

  OakIncrementalIndexRow toOakIncrementalIndexRow(InputRow row)
  {
    int dimsLength;
    row = formatRow(row);
    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, DateTimes.utc(minTimestamp));
    }

    List<String> rowDimensions = row.getDimensions();


    // Updating the dimensionDescs map with new dimensions
    synchronized (dimensionDescs) {

      dimsLength = dimensionDescs.size();

      for (String dimension : rowDimensions) {
        if (Strings.isNullOrEmpty(dimension)) {
          continue;
        }

        if (!dimensionDescs.containsKey(dimension)) {
          ColumnCapabilitiesImpl capabilities = columnCapabilities.get(dimension);
          if (capabilities == null) {
            capabilities = new ColumnCapabilitiesImpl();
            // For schemaless type discovery, assume everything is a String for now, can change later.
            capabilities.setType(ValueType.STRING);
            capabilities.setDictionaryEncoded(true);
            capabilities.setHasBitmapIndexes(true);
            columnCapabilities.put(dimension, capabilities);
          }
          DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dimension, capabilities, null);
          addNewDimension(dimension, capabilities, handler);
          dimsLength++;
        }
      }
    }
    //TODO: handle dimensions that occur more than once in the row

    OakIncrementalIndexRow oakIncrementalIndexInputRow = new OakIncrementalIndexRow(row, dimsLength, this);
    return oakIncrementalIndexInputRow;
  }

  private IncrementalIndexRow getMinIncrementalIndexRow()
  {
    return new IncrementalIndexRow(this.minTimestamp, null, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);
  }

  @Nullable
  @Override
  public String getMetricType(String metric)
  {
    return aggsManager.getMetricType(metric);
  }

  @Override
  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, IncrementalIndexRowHolder currEntry)
  {
    return aggsManager.makeMetricColumnValueSelector(metric, currEntry);
  }

  @Override
  public List<String> getMetricNames()
  {
    return aggsManager.getMetricNames();
  }

  /* ---------------- Serialization utils -------------- */

  static long getTimestamp(ByteBuffer buff)
  {
    return buff.getLong(buff.position() + TIME_STAMP_INDEX);
  }

  static int getDimsLength(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + DIMS_LENGTH_INDEX);
  }

  static int getDimIndexInBuffer(ByteBuffer buff, int dimsLength, int dimIndex)
  {
    if (dimIndex >= dimsLength) {
      return NO_DIM;
    }
    return buff.position() + DIMS_INDEX + dimIndex * ALLOC_PER_DIM;
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex)
  {
    int dimsLength = getDimsLength(buff);
    return getDimValue(buff, dimIndex, dimsLength);
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex, int dimsLength)
  {
    Object dimObject = null;
    if (dimIndex >= dimsLength) {
      return null;
    }
    int dimIndexInBuffer = getDimIndexInBuffer(buff, dimsLength, dimIndex);
    int dimType = buff.getInt(dimIndexInBuffer);
    if (dimType == OakIncrementalIndex.NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = buff.getDouble(dimIndexInBuffer + OakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = buff.getFloat(dimIndexInBuffer + OakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = buff.getLong(dimIndexInBuffer + OakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndexOffset = buff.getInt(dimIndexInBuffer + OakIncrementalIndex.ARRAY_INDEX_OFFSET);
      int arrayIndex = buff.position() + arrayIndexOffset;
      int arraySize = buff.getInt(dimIndexInBuffer + OakIncrementalIndex.ARRAY_LENGTH_OFFSET);
      int[] array = new int[arraySize];
      for (int i = 0; i < arraySize; i++) {
        array[i] = buff.getInt(arrayIndex);
        arrayIndex += Integer.BYTES;
      }
      dimObject = array;
    }

    return dimObject;
  }


  static boolean checkDimsAllNull(ByteBuffer buff, int numComparisons)
  {
    int dimsLength = getDimsLength(buff);
    for (int index = 0; index < Math.min(dimsLength, numComparisons); index++) {
      if (buff.getInt(getDimIndexInBuffer(buff, dimsLength, index)) != OakIncrementalIndex.NO_DIM) {
        return false;
      }
    }
    return true;
  }

  static ValueType getDimValueType(int dimIndex, List<DimensionDesc> dimensionDescsList)
  {
    DimensionDesc dimensionDesc = dimensionDescsList.get(dimIndex);
    if (dimensionDesc == null) {
      return null;
    }
    ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
    if (capabilities == null) {
      return null;
    }
    return capabilities.getType();
  }

  static int getRowIndex(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + ROW_INDEX_INDEX);
  }

}