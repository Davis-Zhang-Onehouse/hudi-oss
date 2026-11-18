/*
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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.SparkKeyGeneratorInterface;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Copies managed objects to independently generated managed files. Existing paths remain ordinary upserts.
 * The streamer hands it the round's instant, already INFLIGHT ({@code hoodie.streamer.start.commit.before.transformer=true}): each
 * managed file is written after its CREATE marker, so a failed round's standard marker-based rollback removes it.
 * Output is persisted and materialized before returning, so copy failures occur inside the streamer's rollback boundary. Content
 * failures carry their identity in one _corrupt_record column for ordinary transformer error routing. The next round
 * releases the previous output after the preceding write has consumed it. A round that ends before the transformer (an
 * unchanged checkpoint) does not release it: the last output stays cached until the next round that reaches the transformer,
 * or until the job ends, so at most one round's output is held.
 */
public class BlobCopyTransformer implements InstantAwareTransformer {

  private static final String CORRUPT_RECORD = BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;
  private transient Dataset<Row> previousOutput;

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props) {
    throw new IllegalStateException("BlobCopyTransformer requires hoodie.streamer.start.commit.before.transformer=true");
  }

  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession spark, StructType schema, TypedProperties props) {
    return errorTableEnabled(props) ? withErrorColumn(schema) : dataSchema(schema);
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props, String instant) {
    if (previousOutput != null) {
      previousOutput.unpersist(false);
      previousOutput = null;
    }
    String basePath = props.getString(HoodieWriteConfig.BASE_PATH.key());
    StructType inputSchema = rows.schema();
    String storageLevel = ConfigUtils.getStringWithAltKeys(props, HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE, true);
    StructType outputSchema = withErrorColumn(inputSchema);
    int corruptIndex = outputSchema.fieldIndex(CORRUPT_RECORD);
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration());
    // a copy buffer the copies cannot use is refused here, on the driver, before any task: every task reads the same value
    BlobTableConfig.copyBufferBytes(props);
    Dataset<Row> output = null;
    try {
      OutOfLineWriter.Factory writers = OutOfLineWriter.forTable(storageConf, basePath, props);
      JavaRDD<Row> copied = rows.toJavaRDD().mapPartitions(partition -> {
        SparkKeyGeneratorInterface keyGenerator = (SparkKeyGeneratorInterface) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
        HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, storageConf.newInstance());
        OutOfLineWriter writer = writers.forInstant(storage, instant);
        return new MappingIterator<>(partition, row -> {
          HoodieKey key = new HoodieKey(keyGenerator.getRecordKey(row), keyGenerator.getPartitionPath(row));
          Object[] values = new Object[outputSchema.size()];
          for (int i = 0; i < row.size(); i++) {
            values[i] = row.get(i);
          }
          if (values[corruptIndex] != null) {
            return RowFactory.create(values);
          }
          try {
            Row result = BlobObjectCopier.copyRow(row, inputSchema, props, key.getPartitionPath(), storage, writer);
            for (int i = 0; i < result.size(); i++) {
              values[i] = result.get(i);
            }
          } catch (BlobCopyFailure failure) {
            if (!failure.isContentFault()) {
              throw failure;
            }
            Map<String, String> identity = new LinkedHashMap<>();
            identity.put("record_key", key.getRecordKey());
            identity.put("partition_path", key.getPartitionPath());
            identity.put("error", failure.getClass().getName() + ": " + failure.getMessage());
            values[corruptIndex] = JsonUtils.toString(identity);
          } catch (IOException failure) {
            throw new HoodieIOException("Blob copy failed at instant " + instant, failure);
          }
          return RowFactory.create(values);
        });
      });
      output = spark.createDataFrame(copied, outputSchema)
          .persist(StorageLevel.fromString(storageLevel));
      output.count();
      if (!errorTableEnabled(props)) {
        List<Row> failures = output.filter(functions.col(CORRUPT_RECORD).isNotNull())
            .select(CORRUPT_RECORD).limit(1).collectAsList();
        if (!failures.isEmpty()) {
          throw new HoodieException("Blob copy failed without an error table: " + failures.get(0).getString(0));
        }
      }
      previousOutput = output;
      return errorTableEnabled(props) ? output : output.drop(CORRUPT_RECORD);
    } catch (Exception failure) {
      if (output != null) {
        output.unpersist(false);
      }
      throw failure instanceof RuntimeException ? (RuntimeException) failure
          : new HoodieException("Blob copy failed at instant " + instant, failure);
    }
  }

  private static boolean errorTableEnabled(TypedProperties props) {
    return ConfigUtils.getBooleanWithAltKeys(props, HoodieErrorTableConfig.ERROR_TABLE_ENABLED);
  }

  private static StructType dataSchema(StructType schema) {
    return new StructType(Arrays.stream(schema.fields())
        .filter(field -> !CORRUPT_RECORD.equals(field.name())).toArray(StructField[]::new));
  }

  private static StructType withErrorColumn(StructType schema) {
    if (!Arrays.asList(schema.fieldNames()).contains(CORRUPT_RECORD)) {
      return schema.add(DataTypes.createStructField(CORRUPT_RECORD, DataTypes.StringType, true));
    }
    return new StructType(Arrays.stream(schema.fields())
        .map(field -> CORRUPT_RECORD.equals(field.name())
            ? new StructField(field.name(), DataTypes.StringType, true, field.metadata()) : field)
        .toArray(StructField[]::new));
  }
}
