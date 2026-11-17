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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.sources.RequiredSourceSettings;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobSourceSetup;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * What the job of {@link UnstructuredDataSource} requires, all of it read from its mode ({@link UnstructuredMode}): the settings
 * it runs with, the mode's transformer in every round, the schema guard last in the chain, and the check of the chain and the
 * schemas against the table's contract.
 */
public class UnstructuredSourceContract implements SourceContract {

  /** The key generators of the job's table: the complex one (partitioned) and the non-partitioned one. */
  private static final List<String> KEY_GENERATORS = Arrays.asList(ComplexKeyGenerator.class.getName(),
      NonpartitionedKeyGenerator.class.getName());

  private final UnstructuredMode mode;

  /**
   * The contract of the job of {@code props}.
   *
   * @throws IllegalArgumentException when {@code hoodie.unstructured.source.mode} is unset or names no mode
   */
  public UnstructuredSourceContract(TypedProperties props) {
    this.mode = UnstructuredMode.from(props);
  }

  /** The job this contract is of. */
  public UnstructuredMode mode() {
    return mode;
  }

  @Override
  public String sourceName() {
    return UnstructuredDataSource.class.getSimpleName();
  }

  /**
   * The settings the job runs with: the round's instant handed to the job's transformer, which writes every row under it
   * ({@code hoodie.streamer.start.commit.before.transformer=true}); the record key of the job's table
   * ({@link UnstructuredMode#keyColumns}) with a key generator of {@link #KEY_GENERATORS}; the job's Parquet defaults
   * ({@link UnstructuredMode#addSettings}); and the job's other checks ({@link #validateJob}) on the merged properties.
   *
   * @param jobProps the job's own properties, read only
   */
  @Override
  public RequiredSourceSettings requiredSettings(TypedProperties jobProps) {
    RequiredSourceSettings.Builder settings = RequiredSourceSettings.builder(UnstructuredSchemaContract.sourceName(mode))
        .requireIgnoringCase(HoodieStreamerConfig.START_COMMIT_BEFORE_TRANSFORMER, "true")
        .requireFields(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, mode.keyColumns(), HoodieTableConfig.RECORDKEY_FIELDS)
        .check(UnstructuredSourceContract::checkKeyGenerator)
        .check((sourceName, merged) -> validateJob(merged));
    return mode.addSettings(settings).build();
  }

  /**
   * Reads the job and the cloud, refuses a key of the other cloud, and validates the job's other properties
   * ({@link BlobSourceSetup#validate}); returns {@code props} unchanged. The streamer runs it with the merge, before it builds
   * anything or creates the table, and the source again as it is built: one copy of the checks.
   *
   * @throws IllegalArgumentException          naming the setting and {@code UnstructuredDataSource (mode ...)}
   */
  public static TypedProperties validateJob(TypedProperties props) {
    UnstructuredMode mode = UnstructuredMode.from(props);
    UnstructuredCloud cloud = UnstructuredCloud.from(props);
    String name = UnstructuredSchemaContract.sourceName(mode);
    cloud.validate(props, name);
    return BlobSourceSetup.validate(props, mode, name);
  }

  /**
   * Refuses a key generator, named or inferred from the record key and partition fields, other than {@link #KEY_GENERATORS}; a name
   * with spaces is also refused, since the writer loads it as written.
   */
  private static void checkKeyGenerator(String sourceName, TypedProperties props) {
    String keyGenerator = HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName(props);
    if (!KEY_GENERATORS.contains(keyGenerator)) {
      throw new IllegalArgumentException(sourceName + " supports only " + String.join(" or ", KEY_GENERATORS) + ", configured: " + keyGenerator);
    }
  }

  @Override
  public Option<Class<? extends Transformer>> requiredTransformer() {
    return Option.of(mode.requiredTransformer());
  }

  /** None while the blob job is the only one ({@link UnstructuredMode#excludedTransformers}). */
  @Override
  public Set<Class<? extends Transformer>> excludedTransformers() {
    return mode.excludedTransformers();
  }

  /** The guard that checks every round's rows against the table's contract, last in the chain. */
  @Override
  public Option<Class<? extends Transformer>> closingTransformer() {
    return Option.of(UnstructuredSchemaGuard.class);
  }

  /**
   * Plans the job's chain on the source's rows and checks it against the table's contract and the user's target schema
   * ({@link UnstructuredSchemaContract#checkChainPlan}): the job's own transformers run after the required one, may add columns
   * and filter rows, and may not drop or change a column they are given.
   */
  @Override
  public void checkWriterSchemas(JavaSparkContext jsc, SparkSession spark, List<String> chain, TypedProperties transformerProps,
                                 Option<HoodieSchema> userTargetSchema, Option<StructType> tableSchema) {
    UnstructuredSchemaContract.checkChainPlan(mode, mode.requiredTransformer(), UnstructuredSchemaGuard.class, chain, jsc, spark,
        transformerProps, userTargetSchema, tableSchema);
  }
}
