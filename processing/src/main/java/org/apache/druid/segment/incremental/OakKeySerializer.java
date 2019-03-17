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

import com.oath.oak.OakSerializer;
import org.apache.druid.segment.column.ValueType;
import java.nio.ByteBuffer;
import java.util.List;


public class OakKeySerializer implements OakSerializer<IncrementalIndexRow>
{

  private final List<IncrementalIndex.DimensionDesc> dimensionDescsList;

  public OakKeySerializer(List<IncrementalIndex.DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }

  @Override
  public void serialize(IncrementalIndexRow incrementalIndexRow, ByteBuffer byteBuffer)
  {
    long timestamp = incrementalIndexRow.getTimestamp();

    int dimsLength = incrementalIndexRow.getDimsLength();
    int rowIndex = incrementalIndexRow.getRowIndex();

    // calculating buffer indexes for writing the key data
    int buffIndex = byteBuffer.position();  // the first byte for writing the key
    int timeStampIndex = buffIndex + OakIncrementalIndex.TIME_STAMP_INDEX;    // the timestamp index
    int dimsLengthIndex = buffIndex + OakIncrementalIndex.DIMS_LENGTH_INDEX;  // the dims array length index
    int rowIndexIndex = buffIndex + OakIncrementalIndex.ROW_INDEX_INDEX;      // the rowIndex index
    int dimsIndex = buffIndex + OakIncrementalIndex.DIMS_INDEX;               // the dims array index
    int dimCapacity = OakIncrementalIndex.ALLOC_PER_DIM;                      // the number of bytes required
    // per dim
    int noDim = OakIncrementalIndex.NO_DIM;                                   // for mentioning that
    // a certain dim is null
    int dimsArraysIndex = dimsIndex + dimCapacity * dimsLength;               // the index for
    // writing the int arrays
    // of dims with a STRING type
    int dimsArrayOffset = dimsArraysIndex - buffIndex;                        // for saving the array position
    // in the buffer
    int valueTypeOffset = OakIncrementalIndex.VALUE_TYPE_OFFSET;              // offset from the dimIndex
    int dataOffset = OakIncrementalIndex.DATA_OFFSET;                         // for non-STRING dims
    int arrayIndexOffset = OakIncrementalIndex.ARRAY_INDEX_OFFSET;            // for STRING dims
    int arrayLengthOffset = OakIncrementalIndex.ARRAY_LENGTH_OFFSET;          // for STRING dims

    byteBuffer.putLong(timeStampIndex, timestamp);
    byteBuffer.putInt(dimsLengthIndex, dimsLength);
    byteBuffer.putInt(rowIndexIndex, rowIndex);
    for (int i = 0; i < dimsLength; i++) {
      ValueType valueType = OakIncrementalIndex.getDimValueType(i, dimensionDescsList);
      if (valueType == null || incrementalIndexRow.getDim(i) == null) {
        byteBuffer.putInt(dimsIndex, noDim);
      } else {
        byteBuffer.putInt(dimsIndex + valueTypeOffset, valueType.ordinal());
        switch (valueType) {
          case FLOAT:
            byteBuffer.putFloat(dimsIndex + dataOffset, (Float) incrementalIndexRow.getDim(i));
            break;
          case DOUBLE:
            byteBuffer.putDouble(dimsIndex + dataOffset, (Double) incrementalIndexRow.getDim(i));
            break;
          case LONG:
            byteBuffer.putLong(dimsIndex + dataOffset, (Long) incrementalIndexRow.getDim(i));
            break;
          case STRING:
            int[] arr = (int[]) incrementalIndexRow.getDim(i);
            byteBuffer.putInt(dimsIndex + arrayIndexOffset, dimsArrayOffset);
            byteBuffer.putInt(dimsIndex + arrayLengthOffset, arr.length);
            for (int arrIndex = 0; arrIndex < arr.length; arrIndex++) {
              byteBuffer.putInt(dimsArraysIndex + arrIndex * Integer.BYTES, arr[arrIndex]);
            }
            dimsArraysIndex += (arr.length * Integer.BYTES);
            dimsArrayOffset += (arr.length * Integer.BYTES);
            break;
          default:
            byteBuffer.putInt(dimsIndex, noDim);
        }
      }

      dimsIndex += dimCapacity;
    }

  }

  @Override
  public IncrementalIndexRow deserialize(ByteBuffer byteBuffer)
  {
    long timeStamp = OakIncrementalIndex.getTimestamp(byteBuffer);
    int dimsLength = OakIncrementalIndex.getDimsLength(byteBuffer);
    int rowIndex = OakIncrementalIndex.getRowIndex(byteBuffer);
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = OakIncrementalIndex.getDimValue(byteBuffer, dimIndex, dimsLength);
      dims[dimIndex] = dim;
    }
    return new IncrementalIndexRow(timeStamp, dims, dimensionDescsList, rowIndex);
  }

  @Override
  public int calculateSize(IncrementalIndexRow incrementalIndexRow)
  {
    //TODO - YONIGO befrore this what == null is it correct now?
    if (incrementalIndexRow.getDimsLength() == 0) {
      return Long.BYTES + 2 * Integer.BYTES;
    }

    // When the dimensionDesc's capabilities are of type ValueType.STRING,
    // the object in timeAndDims.dims is of type int[].
    // In this case, we need to know the array size before allocating the ByteBuffer.
    int sumOfArrayLengths = 0;
    for (int i = 0; i < incrementalIndexRow.getDimsLength(); i++) {
      Object dim = incrementalIndexRow.getDim(i);
      if (dim == null) {
        continue;
      }
      if (OakIncrementalIndex.getDimValueType(i, dimensionDescsList) == ValueType.STRING) {
        sumOfArrayLengths += ((int[]) dim).length;
      }
    }

    // The ByteBuffer will contain:
    // 1. the timeStamp
    // 2. dims.length
    // 3. rowIndex (used for Plain mode only)
    // 4. the serialization of each dim
    // 5. the array (for dims with capabilities of a String ValueType)
    int dimCapacity = OakIncrementalIndex.ALLOC_PER_DIM;
    int allocSize = Long.BYTES + 2 * Integer.BYTES + dimCapacity * incrementalIndexRow.getDimsLength() + Integer.BYTES * sumOfArrayLengths;
    return allocSize;
  }
}