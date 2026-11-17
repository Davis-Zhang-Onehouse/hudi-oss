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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.SourceContracts;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobCopyTransformer;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The contract of {@link UnstructuredDataSource} per mode: the settings, the transformers and the refusals, without Spark. */
class TestUnstructuredSourceContract {

  private static final String START_COMMIT = HoodieStreamerConfig.START_COMMIT_BEFORE_TRANSFORMER.key();
  private static final String CODEC = HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key();
  private static final String BLOOM = HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key();
  private static final String RECORD_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key();
  private static final String KEY_GENERATOR = HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key();
  private static final String BLOB_KEY = "source_path_sha,source_modification_time,source_file_size";

  private static TypedProperties mode(String mode) {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), mode);
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    return props;
  }

  private static UnstructuredSourceContract contract(String mode) {
    return new UnstructuredSourceContract(mode(mode));
  }

  /** What {@code contract}'s settings add to a job that sets only its mode and cloud. */
  private static Map<String, String> settingsOfAnEmptyJob(UnstructuredSourceContract contract) {
    TypedProperties job = mode(contract.mode().configValue());
    Map<String, String> added = asMap(contract.requiredSettings(job).apply(job));
    added.keySet().removeAll(job.stringPropertyNames());
    return added;
  }

  private static Map<String, String> asMap(TypedProperties props) {
    Map<String, String> map = new TreeMap<>();
    for (String key : props.stringPropertyNames()) {
      map.put(key, props.getString(key));
    }
    return map;
  }

  private static String refusal(UnstructuredSourceContract contract, TypedProperties jobProps) {
    return assertThrows(IllegalArgumentException.class, () -> contract.requiredSettings(jobProps).apply(jobProps)).getMessage();
  }

  /** The streamer finds the contract on the source class from its name, before the source is built. */
  @Test
  void resolvedFromTheSourceClass() {
    SourceContract resolved = SourceContracts.of(UnstructuredDataSource.class.getName(), mode("blob"));
    assertTrue(resolved instanceof UnstructuredSourceContract, resolved.getClass().getName());
    assertEquals(UnstructuredMode.BLOB, ((UnstructuredSourceContract) resolved).mode());
    assertEquals("UnstructuredDataSource", resolved.sourceName());
  }

  @Test
  void goldenSettingsBlob() {
    Map<String, String> blob = new TreeMap<>();
    blob.put(START_COMMIT, "true");
    blob.put(RECORD_KEY, BLOB_KEY);
    blob.put(CODEC, "uncompressed");
    blob.put(BLOOM, "false");
    UnstructuredSourceContract contract = contract("blob");
    assertEquals(blob, settingsOfAnEmptyJob(contract));
    assertKeyGenerators(contract);
    assertEquals(BLOB_KEY, String.join(",", BlobTableSchema.KEY_COLUMNS));

    TypedProperties own = mode("blob");
    own.setProperty(CODEC, "zstd");
    own.setProperty(BLOOM, "true");
    TypedProperties kept = contract.requiredSettings(own).apply(own);
    assertEquals("zstd", kept.getString(CODEC), "the codec is a default");
    assertEquals("true", kept.getString(BLOOM), "the bloom filter is a default");
  }

  /** The complex or the non-partitioned key generator, named or inferred; any other is refused, naming the source and its mode. */
  private static void assertKeyGenerators(UnstructuredSourceContract contract) {
    for (String accepted : new String[] {ComplexKeyGenerator.class.getName(), NonpartitionedKeyGenerator.class.getName()}) {
      TypedProperties props = mode(contract.mode().configValue());
      props.setProperty(KEY_GENERATOR, accepted);
      contract.requiredSettings(props).apply(props);
    }
    TypedProperties partitioned = mode(contract.mode().configValue());
    partitioned.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "source_file_extension");
    contract.requiredSettings(partitioned).apply(partitioned);
    for (String refused : new String[] {SimpleKeyGenerator.class.getName(), " " + ComplexKeyGenerator.class.getName() + " "}) {
      TypedProperties props = mode(contract.mode().configValue());
      props.setProperty(KEY_GENERATOR, refused);
      assertEquals(UnstructuredDataSource.name(contract.mode()) + " supports only " + ComplexKeyGenerator.class.getName() + " or "
          + NonpartitionedKeyGenerator.class.getName() + ", configured: " + refused, refusal(contract, props));
    }
  }

  @Test
  void transformersPerMode() {
    UnstructuredSourceContract blob = contract("blob");
    assertEquals(Option.of(BlobCopyTransformer.class), blob.requiredTransformer());
    assertEquals(Collections.emptySet(), blob.excludedTransformers());
    assertEquals(Option.of(UnstructuredSchemaGuard.class), blob.closingTransformer());
    assertEquals(UnstructuredMode.BLOB, blob.mode());
  }

  /** A job that sets every required setting itself, to the same value, keeps its properties: the merge changes none of them. */
  @Test
  void jobThatSetsEveryRequiredKeyToTheSameValueStarts() {
    TypedProperties props = mode("blob");
    props.setProperty(START_COMMIT, "true");
    props.setProperty(RECORD_KEY, BLOB_KEY);
    props.setProperty(KEY_GENERATOR, ComplexKeyGenerator.class.getName());
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "source_file_extension");
    props.setProperty(CODEC, "uncompressed");
    props.setProperty(BLOOM, "false");
    TypedProperties merged = new UnstructuredSourceContract(props).requiredSettings(props).apply(props);
    assertEquals(asMap(props), asMap(merged), "mode blob");
  }

  /**
   * The job's props-only checks run with the merge, at the top of the streamer, before the table is created: the same refusals the
   * source makes as it is built, from one copy of the checks.
   */
  @Test
  void jobChecksRunWithTheMerge() {
    TypedProperties gcsWithS3Prefix = mode("blob");
    gcsWithS3Prefix.setProperty(UnstructuredSourceConfig.CLOUD.key(), "gcs");
    gcsWithS3Prefix.setProperty("hoodie.streamer.source.s3incr.fs.prefix", "s3a");
    assertTrue(assertThrows(IllegalArgumentException.class, () -> new UnstructuredSourceContract(gcsWithS3Prefix)
        .requiredSettings(gcsWithS3Prefix).apply(gcsWithS3Prefix)).getMessage()
        .startsWith("UnstructuredDataSource (mode blob) refuses hoodie.streamer.source.s3incr.fs.prefix"));
  }

  @Test
  void refusalsNameUnstructuredDataSourceAndMode() {
    TypedProperties startCommit = new TypedProperties();
    startCommit.setProperty(START_COMMIT, "false");
    assertEquals("UnstructuredDataSource (mode blob) requires " + START_COMMIT + "=true, configured: false",
        refusal(contract("blob"), startCommit));
    TypedProperties otherKey = new TypedProperties();
    otherKey.setProperty(RECORD_KEY, "source_path_sha");
    assertEquals("UnstructuredDataSource (mode blob) requires " + RECORD_KEY + "=" + BLOB_KEY + " (these fields, in this order),"
        + " configured: source_path_sha", refusal(contract("blob"), otherKey));

    HoodieTableConfig otherTable = new HoodieTableConfig();
    otherTable.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "source_path_sha");
    IllegalArgumentException table = assertThrows(IllegalArgumentException.class,
        () -> contract("blob").requiredSettings(new TypedProperties()).checkTable("file:/tables/other", otherTable));
    assertEquals("UnstructuredDataSource (mode blob) requires the table's " + HoodieTableConfig.RECORDKEY_FIELDS.key() + "="
        + BLOB_KEY + ": the table at file:/tables/other stores source_path_sha; write this job to a new table", table.getMessage());

    // the contract is built from the mode alone: an unset or unknown mode is refused as the contract is built
    assertTrue(assertThrows(IllegalArgumentException.class, () -> new UnstructuredSourceContract(new TypedProperties())).getMessage()
        .startsWith("UnstructuredDataSource requires " + UnstructuredSourceConfig.MODE.key() + ", one of: "));
    assertThrows(IllegalArgumentException.class, () -> SourceContracts.of(UnstructuredDataSource.class.getName(), mode("video")),
        "the contract constructor's refusal reaches the caller as thrown");
  }
}
