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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex.DimensionDesc;

import java.util.List;

public class OakIncrementalIndexRow extends IncrementalIndexRow
{

  public static final long EMPTY_TIME_STAMP = -1;
  protected OakIncrementalIndex index;
  private InputRow inputRow;
  private int dimsLength;
  private Object[] rawDims;

  public OakIncrementalIndexRow(InputRow inputRow, int dimsLength, OakIncrementalIndex index)
  {
    this.timestamp = EMPTY_TIME_STAMP;
    this.index = index;
    this.inputRow = inputRow;
    this.dimsLength = dimsLength;
    this.dims = null;
    this.rawDims = null;
    this.rowIndex = EMPTY_ROW_INDEX;
    this.dimensionDescsList = index.dimensionDescsList;
  }

  @Override
  public long getTimestamp()
  {
    if (timestamp == EMPTY_TIME_STAMP) {
      long truncated = 0;
      if (inputRow.getTimestamp() != null) {
        truncated = index.getGranularity().bucketStart(inputRow.getTimestamp()).getMillis();
      }

      timestamp = Math.max(truncated, index.getMinTimestamp());
    }

    return timestamp;
  }

  @Override
  public int getDimsLength()
  {
    return dimsLength;
  }

  @Override
  public Object getDim(int dimIndex)
  {
    if (dimIndex >= getDimsLength()) {
      return null;
    }

    if (dims == null) {
      dims = new Object[dimsLength];
    }

    if (dims[dimIndex] == null) {

      DimensionDesc desc = index.dimensionDescsList.get(dimIndex);
      if (desc == null) { // assuming the DimensionDesc has already been initialized
        return null;
      }


      if (rawDims == null) {
        rawDims = new Object[dimsLength];
      }

      if (rawDims[dimIndex] == null) {
        rawDims[dimIndex] = inputRow.getRaw(desc.getName());
      }

      //TODO: handle parse exceptions
      DimensionIndexer indexer = desc.getIndexer();
      dims[dimIndex] = indexer.processRowValsToUnsortedEncodedKeyComponent(rawDims[dimIndex], false);
    }


    return dims[dimIndex];
  }

  public Object getRawDim(int dimIndex)
  {
    DimensionDesc desc = index.dimensionDescsList.get(dimIndex);
    if (desc == null) { // assuming the DimensionDesc has already been initialized
      return null;
    }
    if (rawDims == null) {
      rawDims = new Object[dimsLength];
    }

    if (rawDims[dimIndex] == null) {
      rawDims[dimIndex] = inputRow.getRaw(desc.getName());
    }

    return rawDims[dimIndex];
  }

  public Object getExistingDim(int dimIndex)
  {
    if (dims == null) {
      return null;
    }
    return dims[dimIndex];
  }

  @Override
  public Object[] getDims()
  {
    return dims;
  }

  @Override
  public int getRowIndex()
  {
    return rowIndex;
  }

  @Override
  void setRowIndex(int rowIndex)
  {
    this.rowIndex = rowIndex;
  }

  @Override
  public int calcStringDimSize(int dimIndex)
  {
    DimensionDesc desc = index.dimensionDescsList.get(dimIndex);

    if (desc == null) {
      return 0; // assuming the DimensionDesc has already been initialized
    }

    if (rawDims == null) {
      rawDims = new Object[dimsLength];
    }

    if (rawDims[dimIndex] == null) {
      rawDims[dimIndex] = inputRow.getRaw(desc.getName());
    }

    if (rawDims[dimIndex] == null) {
      return Integer.BYTES;
    } else if (rawDims[dimIndex] instanceof List) {
      List<Object> dimValuesList = (List) rawDims[dimIndex];
      if (dimValuesList.isEmpty()) {
        return 0;
      } else {
        return Integer.BYTES * dimValuesList.size();
      }
    } else {
      return Integer.BYTES;
    }
  }

  /**
   * bytesInMemory estimates the size of the serialized IncrementalIndexRow key.
   * Each serialized IncrementalRoeIndex contains:
   * 1. a timeStamp
   * 2. the dims array length
   * 3. the rowIndex
   * 4. the serialization of each dim
   * 5. the array (for dims with capabilities of a String ValueType)
   *
   * @return long estimated bytesInMemory
   */
  @Override
  public long estimateBytesInMemory()
  {

    long sizeInBytes = Long.BYTES + 2 * Integer.BYTES;
    for (String dim : inputRow.getDimensions()) {
      DimensionDesc desc = index.dimensionDescs.get(dim);
      if (desc == null) {
        continue;
      }
      sizeInBytes += OakIncrementalIndex.ALLOC_PER_DIM;
      ColumnCapabilitiesImpl capabilities = desc.getCapabilities();
      ValueType valueType = capabilities.getType();
      if (valueType == ValueType.STRING) {
        int dimIndex = desc.getIndex();
        sizeInBytes += calcStringDimSize(dimIndex);
      }
    }

    return sizeInBytes;
  }


}
