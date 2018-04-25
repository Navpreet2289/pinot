/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.aggregate.ValueAggregator;
import com.linkedin.pinot.core.data.readers.aggregate.ValueAggregatorFactory;
import com.linkedin.pinot.core.operator.transform.transformer.datetime.EpochToEpochTransformer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Record reader that rolls records up on the time column.
 *
 * <p>Limitation
 * <ul>
 *   <li>Time column data type has to be 'long' if one needs to change the time granularity</li>
 *   <li>No support for tables with multi-value or no-dictionary columns yet</li>
 * </ul>
 */
public class MultipleRollupPinotSegmentRecordReader implements RecordReader {
  private MultiplePinotSegmentRecordReader _multipleSegmentRecordReader;
  private Schema _schema;
  private GenericRow _currentRow;
  private Map<String, String> _aggregatorTypeMap;
  private EpochToEpochTransformer _dateTimeTransformer;

  private final static String DEFAULT_AGGREGATOR_TYPE = "SUM";

  /**
   * Read records using the passed in schema and in the order of sorted column from multiple pinot segments.
   * <p>Passed in schema must be a subset of the segment schema.
   * <p>If rollup segment record reader config is not given, we sort in the order of arbitrary dimension columns and
   * then the time column and we use "SUM" function for aggregation by default.
   *
   * @param indexDirs a list of input paths for the segment indices
   * @param schema input schema that is a subset of the segment schema
   * @param config configuration for roll-up pinot segment record reader
   */
  public MultipleRollupPinotSegmentRecordReader(@Nonnull List<File> indexDirs, @Nonnull Schema schema,
      @Nullable MultipleRollupPinotSegmentRecordReaderConfig config) throws Exception {
    _schema = schema;
    List<String> newSortOrder;
    if (config == null) {
      newSortOrder = getSortOrder(schema, null);
      _aggregatorTypeMap = new HashMap<>();
    } else {
      _aggregatorTypeMap = (config.getAggregatorTypeMap() != null) ? config.getAggregatorTypeMap() : new HashMap<>();
      _dateTimeTransformer = config.getDateTimeTransformer();
      newSortOrder = getSortOrder(schema, config.getSortOrder());
    }
    _multipleSegmentRecordReader = new MultiplePinotSegmentRecordReader(indexDirs, _schema, newSortOrder);

    if (_multipleSegmentRecordReader.hasNext()) {
      _currentRow = convertTimeGranularity(_multipleSegmentRecordReader.next());
    }
  }

  /**
   * Initialize the sorting order. Because we need to sort using all dimension columns and a time column, we compute a
   * new sorting order as follows:
   * 1. Put dimension columns in a given sort sorder
   * 2. Put all the rest of dimension columns
   * 3. Put time column at the last
   */
  private List<String> getSortOrder(@Nonnull Schema schema, @Nullable List<String> sortOrder) {
    List<String> newSortOrder = new ArrayList<>();
    String timeColumnName = schema.getTimeColumnName();

    // Put all columns from sortOrder list to new sorting order
    if (sortOrder != null && !sortOrder.isEmpty() && timeColumnName != null) {
      if (sortOrder.contains(timeColumnName)) {
        throw new IllegalStateException(
            "Time column should not be included in the sort order for roll-up record reader. It is always included as "
                + "the last order");
      }
      newSortOrder.addAll(sortOrder);
    }
    // Add rest of dimension columns
    for (FieldSpec fieldSpec : _schema.getDimensionFieldSpecs()) {
      String dimensionName = fieldSpec.getName();
      if (sortOrder == null || !sortOrder.contains(dimensionName)) {
        newSortOrder.add(dimensionName);
      }
    }
    // Add time column
    if (timeColumnName != null) {
      newSortOrder.add(timeColumnName);
    }
    return newSortOrder;
  }

  @Override
  public boolean hasNext() {
    return _currentRow != null;
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) throws IOException {
    // Copy the current row to reuse, which will be the final result
    RecordReaderUtils.copyRow(_currentRow, reuse);

    while (true) {
      // If there is no more row left, mark the current row to be null and break the loop
      if (!_multipleSegmentRecordReader.hasNext()) {
        _currentRow = null;
        break;
      }

      // Read the current row
      _currentRow = convertTimeGranularity(_multipleSegmentRecordReader.next(_currentRow));

      // If the dimension and timcolumn value is not the same, break the loop to return the result
      if (!haveSameDimensionAndTimeColumn(reuse, _currentRow)) {
        break;
      } else {
        // Since the dimension and time columns are the same, metric values need to be aggregated
        for (MetricFieldSpec metric : _schema.getMetricFieldSpecs()) {
          String metricName = metric.getName();
          String aggregatorType = _aggregatorTypeMap.getOrDefault(metricName, DEFAULT_AGGREGATOR_TYPE);
          ValueAggregator aggregator = ValueAggregatorFactory.getRowAggregator(aggregatorType);
          Object aggregatedResult =
              aggregator.aggregate(reuse.getValue(metricName), _currentRow.getValue(metricName), metric);
          reuse.putField(metricName, aggregatedResult);
        }
      }
    }
    return reuse;
  }

  @Override
  public void rewind() throws IOException {
    _multipleSegmentRecordReader.rewind();
    if (_multipleSegmentRecordReader.hasNext()) {
      _currentRow = convertTimeGranularity(_multipleSegmentRecordReader.next());
    }
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close() throws IOException {
    _multipleSegmentRecordReader.close();
  }

  /**
   * Helper function to convert the time column value based on the input date time transformer
   */
  private GenericRow convertTimeGranularity(GenericRow row) {
    String timeColumn = _schema.getTimeColumnName();
    if (_dateTimeTransformer == null || timeColumn == null) {
      return row;
    }
    FieldSpec.DataType dataType = _schema.getTimeFieldSpec().getDataType();
    if (dataType != FieldSpec.DataType.LONG) {
      throw new IllegalStateException(
          "Time column granularity change is only supported for a time column with long data type");
    }
    long[] input = new long[1];
    long[] output = new long[1];
    input[0] = (Long) row.getValue(_schema.getTimeColumnName());
    _dateTimeTransformer.transform(input, output, 1);
    row.putField(timeColumn, output[0]);
    return row;
  }

  /**
   * Check that two rows are having the same dimension and time column values.
   */
  private boolean haveSameDimensionAndTimeColumn(GenericRow row1, GenericRow row2) {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
        String columnName = fieldSpec.getName();
        Object value1 = row1.getValue(columnName);
        Object value2 = row2.getValue(columnName);
        if (!value1.equals(value2)) {
          return false;
        }
      }
    }
    return true;
  }
}
