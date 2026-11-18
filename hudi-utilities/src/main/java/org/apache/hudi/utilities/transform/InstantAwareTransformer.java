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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link Transformer} that can be given the instant the round writes to.
 *
 * <p>With {@code hoodie.streamer.start.commit.before.transformer} on, the streamer starts the commit and moves it to INFLIGHT
 * (under LAZY failed-writes cleaning it also has a heartbeat) before it transforms, and calls
 * {@link #apply(JavaSparkContext, SparkSession, Dataset, TypedProperties, String)} with that instant. A file the transformer
 * writes for the instant follows the write protocol: its CREATE marker first, then the file, so a rollback of the instant
 * removes it.
 * With the flag off the streamer calls the plain {@link #apply(JavaSparkContext, SparkSession, Dataset, TypedProperties)}
 * before any instant exists.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface InstantAwareTransformer extends Transformer {

  /**
   * Transforms the source dataset for the round that writes to {@code instantTime}.
   *
   * @param jsc          JavaSparkContext
   * @param sparkSession Spark Session
   * @param rowDataset   Source DataSet
   * @param properties   Config properties
   * @param instantTime  the INFLIGHT instant of the round
   * @return Transformed Dataset
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties, String instantTime);
}
