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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.transform.RequiredTransformerChain;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * What a source's job requires: the settings it runs with, the transformers its rows must pass, and the schemas it may write. A
 * source class names its contract with {@link SourceContractClass}; the streamer builds the contract from the job's properties
 * alone ({@link SourceContracts#of}), before anything reads those properties and before the source. A contract holds no
 * resources and reads no data, so a job it refuses never builds its source.
 *
 * <p>The streamer merges {@link #requiredSettings} into a copy of the job's properties first, and every later step reads that
 * copy: the key generator, the write config, the metrics, the error table, the table, and the source itself. The source is
 * built from the merged properties, so it sees the settings the writer uses.
 */
public interface SourceContract {

  /**
   * The settings the job runs with ({@link RequiredSourceSettings#apply}): an unset setting is set, the same value is accepted, a
   * different one refuses the job at start.
   *
   * @param jobProps the job's own properties, read only
   */
  RequiredSourceSettings requiredSettings(TypedProperties jobProps);

  /** The name refusals give the source, such as its class's simple name. */
  default String sourceName() {
    return getClass().getSimpleName();
  }

  /**
   * The transformer that must run on every round of the source, a subclass of it counting as it ({@link RequiredTransformerChain});
   * none by default.
   */
  default Option<Class<? extends Transformer>> requiredTransformer() {
    return Option.empty();
  }

  /** Transformers that must not run with the source; a job whose chain names one (or a subclass) is refused at start. */
  default Set<Class<? extends Transformer>> excludedTransformers() {
    return Collections.emptySet();
  }

  /** A transformer the streamer runs last in every round, after the job's own; a job may not name it. None by default. */
  default Option<Class<? extends Transformer>> closingTransformer() {
    return Option.empty();
  }

  /**
   * Checks the chain and the schemas the job writes with, without reading data, before the source is built. Nothing by default.
   *
   * @param chain            the composed {@code --transformer-class} entries, the required and closing ones included
   * @param props            the properties the transformers run with
   * @param userTargetSchema the job's target schema, when its schema provider gives one
   * @param tableSchema      the existing table's latest writer schema, without meta columns; empty for a new table
   * @throws org.apache.hudi.common.schema.internal.HoodieSchemaException naming the transformer or the column and the rule
   */
  default void checkWriterSchemas(JavaSparkContext jsc, SparkSession spark, List<String> chain, TypedProperties props,
                                  Option<HoodieSchema> userTargetSchema, Option<StructType> tableSchema) {
  }
}
