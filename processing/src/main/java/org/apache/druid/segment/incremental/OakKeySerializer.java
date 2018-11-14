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

import java.util.List;
import java.nio.ByteBuffer;

import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import com.oath.oak.OakSerializer;

public class OakKeySerializer implements OakSerializer<IncrementalIndexRow>
{
  private List<DimensionDesc> dimensionDescsList;

  public OakKeySerializer(List<DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }

  @Override
  public void serialize(IncrementalIndexRow incrementalIndexRow, ByteBuffer buff)
  {
    OakIncrementalIndexRow oakIncrementalIndexRow = (OakIncrementalIndexRow) incrementalIndexRow;
    long timestamp = incrementalIndexRow.getTimestamp();
    int dimsLength = oakIncrementalIndexRow.getDimsLength();
    int rowIndex = incrementalIndexRow.getRowIndex();

    // calculating buffer indexes for writing the key data
    int buffIndex = buff.position();  // the first byte for writing the key
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

    buff.putLong(timeStampIndex, timestamp);
    buff.putInt(dimsLengthIndex, dimsLength);
    buff.putInt(rowIndexIndex, rowIndex);
    for (int i = 0; i < dimsLength; i++) {
      ValueType valueType = OakIncrementalIndex.getDimValueType(i, dimensionDescsList);
      if (valueType == null) {
        buff.putInt(dimsIndex, noDim);
      } else {
        buff.putInt(dimsIndex + valueTypeOffset, valueType.ordinal());
        Object dim = oakIncrementalIndexRow.getExistingDim(i);
        if (dim != null) {

          buff.putInt(dimsIndex + valueTypeOffset, valueType.ordinal());
          switch (valueType) {
            case FLOAT:
              buff.putFloat(dimsIndex + dataOffset, (Float) dim);
              break;
            case DOUBLE:
              buff.putDouble(dimsIndex + dataOffset, (Double) dim);
              break;
            case LONG:
              buff.putLong(dimsIndex + dataOffset, (Long) dim);
              break;
            case STRING:
              int[] arr = (int[]) dim;
              buff.putInt(dimsIndex + arrayIndexOffset, dimsArrayOffset);
              buff.putInt(dimsIndex + arrayLengthOffset, arr.length);
              for (int arrIndex = 0; arrIndex < arr.length; arrIndex++) {
                buff.putInt(dimsArraysIndex + arrIndex * Integer.BYTES, arr[arrIndex]);
              }
              dimsArraysIndex += (arr.length * Integer.BYTES);
              dimsArrayOffset += (arr.length * Integer.BYTES);
              break;
            default:
              buff.putInt(dimsIndex, noDim);
          }


        } else {
          DimensionDesc desc = dimensionDescsList.get(i);
          if (desc == null) { // assuming the DimensionDesc has already been initialized
            buff.putInt(dimsIndex, noDim);
          } else {
            DimensionIndexer indexer = desc.getIndexer();
            Object rawDim = oakIncrementalIndexRow.getRawDim(i);
            if (rawDim == null) {
              buff.putInt(dimsIndex, noDim);
            } else {
              if (valueType == ValueType.FLOAT || valueType == ValueType.DOUBLE || valueType == ValueType.LONG) {
                ByteBuffer chunk = buff.duplicate();
                chunk.position(dimsIndex + dataOffset);
                chunk.limit(dimsIndex + dataOffset + dimCapacity);
                chunk = chunk.slice();
                indexer.writeUnsortedEncodedKeyComponent(rawDim, false, chunk);
              } else if (valueType == ValueType.STRING) {
                buff.putInt(dimsIndex + arrayIndexOffset, dimsArrayOffset);
                ByteBuffer chunk = buff.duplicate();
                chunk.position(dimsArraysIndex);
                chunk.limit(dimsArraysIndex + oakIncrementalIndexRow.calcStringDimSize(i));
                chunk = chunk.slice();
                int arrSize = (int) indexer.writeUnsortedEncodedKeyComponent(rawDim, false, chunk);
                buff.putInt(dimsIndex + arrayLengthOffset, arrSize / Integer.BYTES);
                dimsArraysIndex += arrSize;
                dimsArrayOffset += arrSize;
              } else {
                buff.putInt(dimsIndex, noDim);
              }
            }
          }
        }
      }

      dimsIndex += dimCapacity;
    }

  }

  @Override
  public IncrementalIndexRow deserialize(ByteBuffer serializedKey)
  {
    long timeStamp = OakIncrementalIndex.getTimestamp(serializedKey);
    int dimsLength = OakIncrementalIndex.getDimsLength(serializedKey);
    int rowIndex = OakIncrementalIndex.getRowIndex(serializedKey);
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = OakIncrementalIndex.getDimValue(serializedKey, dimIndex, dimsLength);
      dims[dimIndex] = dim;
    }
    return new IncrementalIndexRow(timeStamp, dims, dimensionDescsList, rowIndex);
  }

  @Override
  public int calculateSize(IncrementalIndexRow incrementalIndexRow)
  {
    OakIncrementalIndexRow oakIncrementalIndexRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return (int) oakIncrementalIndexRow.estimateBytesInMemory();
  }
}
