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

package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class FloatDimensionIndexer implements DimensionIndexer<Float, Float, Float>
{
  public static final Comparator<Float> FLOAT_COMPARATOR = Comparators.naturalNullsFirst();

  @Override
  public Float processRowValsToUnsortedEncodedKeyComponent(Object dimValues, boolean reportParseExceptions)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }

    return DimensionHandlerUtils.convertObjectToFloat(dimValues, reportParseExceptions);
  }

  @Override
  public long estimateRowValsSize(Object dimValues)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }
    return Float.BYTES;
  }

  @Override
  public long writeUnsortedEncodedKeyComponent(Object dimValues, boolean reportParseExceptions, ByteBuffer buff)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }

    Float key = DimensionHandlerUtils.convertObjectToFloat(dimValues, reportParseExceptions);
    buff.putFloat(buff.position(), key);
    return Float.BYTES;
  }

  @Override
  public long estimateEncodedKeyComponentSize(Float key)
  {
    return Float.BYTES;
  }

  @Override
  public Float getUnsortedEncodedValueFromSorted(Float sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public CloseableIndexed<Float> getSortedIndexedValues()
  {
    throw new UnsupportedOperationException("Numeric columns do not support value dictionaries.");
  }

  @Override
  public Float getMinValue()
  {
    return Float.NEGATIVE_INFINITY;
  }

  @Override
  public Float getMaxValue()
  {
    return Float.POSITIVE_INFINITY;
  }

  @Override
  public int getCardinality()
  {
    return DimensionSelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    return new FloatWrappingDimensionSelector(makeColumnValueSelector(currEntry, desc), spec.getExtractionFn());
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerFloatColumnSelector implements FloatColumnSelector
    {

      @Override
      public boolean isNull()
      {
        return currEntry.get().getDim(dimIndex) == null;
      }

      @Override
      public float getFloat()
      {
        final Object dim = currEntry.get().getDim(dimIndex);

        if (dim == null) {
          assert NullHandling.replaceWithDefault();
          return 0.0f;
        }

        return (Float) dim;
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Float getObject()
      {
        final Object dim = currEntry.get().getDim(dimIndex);
        return dim == null ? null : (Float) dim;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }

    return new IndexerFloatColumnSelector();
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(@Nullable Float lhs, @Nullable Float rhs)
  {
    return FLOAT_COMPARATOR.compare(lhs, rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(@Nullable Float lhs, @Nullable Float rhs)
  {
    return Objects.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable Float key)
  {
    return DimensionHandlerUtils.nullToZero(key).hashCode();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(Float key)
  {
    return key;
  }

  @Override
  public ColumnValueSelector convertUnsortedValuesToSorted(ColumnValueSelector selectorWithUnsortedValues)
  {
    return selectorWithUnsortedValues;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Float key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Numeric columns do not support bitmaps.");
  }
}
