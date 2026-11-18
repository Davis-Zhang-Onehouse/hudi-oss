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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Transform source to target dataset before writing.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public interface Transformer {

  /**
   * Transform source RDD to target RDD.
   *
   * @param jsc JavaSparkContext
   * @param sparkSession Spark Session
   * @param rowDataset Source DataSet
   * @param properties Config properties
   * @return Transformed Dataset
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties);

  /**
   * Invoked on the driver after a durable table commit and the source's commit callback. The default performs no action.
   * A failure stops post-commit processing; the completed table commit is retained.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default void onCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime) {
  }

  /** Driver validation inside the data-table commit transaction. Implementations must not reacquire the table lock. */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default void onPreCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime,
                           HoodieTableMetaClient table) {
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    Dataset<Row> emptyDataset = sparkSession.createDataFrame(sparkSession.emptyDataFrame().rdd(), incomingStruct);
    Dataset<Row> transformedDataset = this.apply(jsc, sparkSession, emptyDataset, properties);
    return transformedDataset.schema();
  }
}
