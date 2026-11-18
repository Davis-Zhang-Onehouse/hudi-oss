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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.SourceContracts;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobParquetConfigInjector;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingParquetConfigInjector;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The contract of {@link UnstructuredDataSource} per mode: the settings, the transformers and the refusals, without Spark. */
class TestUnstructuredSourceContract {

  private static final String INJECTOR = HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key();
  private static final String CODEC = HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key();
  private static final String BLOOM = HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key();
  private static final String RECORD_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key();
  private static final String KEY_GENERATOR = HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key();
  private static final String MARKERS = HoodieWriteConfig.MARKERS_TYPE.key();
  private static final String ROLLBACK = HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key();
  private static final String BLOB_KEY = "source_path_sha,source_modification_time,source_file_size";
  private static final String EMBEDDING_KEY = BLOB_KEY + ",modality,chunk_id";

  private static TypedProperties mode(String mode) {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), mode);
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    if ("embedding".equals(mode)) {
      props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    }
    return props;
  }

  /** Builds the contract of {@code props} and prepares the job with it: a refusal of either step is thrown. */
  private static void prepareJob(TypedProperties props) {
    new UnstructuredSourceContract(props).prepareJob(props);
  }

  private static UnstructuredSourceContract contract(String mode) {
    return new UnstructuredSourceContract(mode(mode));
  }

  /** What {@code contract}'s settings add to a job that sets only its mode and cloud. */
  private static Map<String, String> settingsOfAnEmptyJob(UnstructuredSourceContract contract) {
    TypedProperties job = mode(contract.mode().configValue());
    Map<String, String> added = asMap(contract.prepareJob(job));
    added.keySet().removeAll(job.stringPropertyNames());
    for (String kind : new String[] {"text", "image", "video", "audio"}) {
      String version = added.get("hoodie.table.embedding.prep." + kind + ".logic.version");
      if (contract.mode() == UnstructuredMode.EMBEDDING) {
        assertTrue(version.startsWith("prep-logic-v3;") && version.contains(";source-version-check=mtime-size-v1"), kind);
      }
    }
    // the embedding prep contract is its own family of table properties (TestPrepLogicVersions)
    added.keySet().removeIf(key -> key.startsWith("hoodie.table.embedding.prep."));
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
    return assertThrows(IllegalArgumentException.class, () -> contract.prepareJob(jobProps)).getMessage();
  }

  /** The streamer finds the contract on the source class from its name, before the source is built. */
  @Test
  void resolvedFromTheSourceClass() {
    SourceContract resolved = SourceContracts.of(UnstructuredDataSource.class.getName(), mode("embedding"));
    assertTrue(resolved instanceof UnstructuredSourceContract, resolved.getClass().getName());
    assertEquals(UnstructuredMode.EMBEDDING, ((UnstructuredSourceContract) resolved).mode());
  }

  @Test
  void tableCreationPinsBlobLayoutAndSchemaVersions() {
    TypedProperties props = mode("blob");
    TypedProperties merged = contract("blob").prepareJob(props);
    assertEquals("1", merged.getProperty("hoodie.table.blob.layout.version"));
    assertEquals("blob", merged.getProperty("hoodie.table.unstructured.kind"));
    assertEquals("1", merged.getProperty("hoodie.table.unstructured.schema.version"));
  }

  @Test
  void unknownBlobLayoutVersionIsRefused() {
    HoodieTableConfig table = new HoodieTableConfig();
    table.setValue(HoodieTableConfig.RECORDKEY_FIELDS, BLOB_KEY);
    table.getProps().setProperty("hoodie.table.blob.layout.version", "99");
    UnstructuredTableProperties required = contract("blob").requiredTableProperties();
    assertThrows(IllegalArgumentException.class, () -> required.checkTable("file:/tables/blob", table));
  }

  @Test
  void embeddingCreationPinsLogicAndRefusesChangedLogic() {
    TypedProperties props = mode("embedding");
    UnstructuredTableProperties settings = contract("embedding").requiredTableProperties();
    TypedProperties merged = settings.apply(props);
    assertEquals("embedding", merged.getProperty("hoodie.table.unstructured.kind"));
    for (String kind : new String[] {"text", "image", "video", "audio"}) {
      String key = "hoodie.table.embedding.prep." + kind + ".logic.version";
      assertTrue(merged.containsKey(key), key);
      HoodieTableConfig table = new HoodieTableConfig();
      table.getProps().putAll(merged);
      table.setValue(HoodieTableConfig.RECORDKEY_FIELDS, EMBEDDING_KEY);
      settings.checkTable("file:/tables/embedding", table);
      table.getProps().setProperty(key, "other-logic");
      assertThrows(IllegalArgumentException.class,
          () -> settings.checkTable("file:/tables/embedding", table), key);
    }
  }

  /** The embedding contract is checked key by key: a setting an older table does not store compares as its default. */
  @Test
  void embeddingTableWithoutANewerSettingKeepsItsDefault() {
    UnstructuredTableProperties settings = contract("embedding").requiredTableProperties();
    HoodieTableConfig table = new HoodieTableConfig();
    table.getProps().putAll(settings.tableProperties());
    table.setValue(HoodieTableConfig.RECORDKEY_FIELDS, EMBEDDING_KEY);
    table.getProps().remove("hoodie.table.embedding.prep.chunk.max.words");
    settings.checkTable("file:/tables/embedding", table);
    table.getProps().setProperty("hoodie.table.embedding.prep.chunk.max.tokens", "128");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> settings.checkTable("file:/tables/embedding", table));
    assertTrue(refused.getMessage().contains("hoodie.table.embedding.prep.chunk.max.tokens=256 (set by hoodie.unstructured.chunk.max.tokens;"
        + " the table stores 128)"), refused.getMessage());
    assertTrue(refused.getMessage().endsWith("write this job to a new table"), refused.getMessage());
  }

  @Test
  void goldenSettingsBlob() {
    Map<String, String> blob = new TreeMap<>();
    blob.put("hoodie.table.blob.layout.version", "1");
    blob.put("hoodie.table.unstructured.kind", "blob");
    blob.put("hoodie.table.unstructured.schema.version", "1");
    blob.put("hoodie.table.unstructured.key.encoding.version", "1");
    blob.put(RECORD_KEY, BLOB_KEY);
    blob.put(MARKERS, "TIMELINE_SERVER_BASED");
    blob.put(ROLLBACK, "true");
    blob.put(INJECTOR, BlobParquetConfigInjector.class.getName());
    blob.put(CODEC, "uncompressed");
    blob.put(BLOOM, "false");
    UnstructuredSourceContract contract = contract("blob");
    assertEquals(blob, settingsOfAnEmptyJob(contract));
    assertKeyGenerators(contract);

    TypedProperties markersOff = mode("blob");
    markersOff.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    markersOff.setProperty(MARKERS, "DIRECT");
    TypedProperties merged = contract.prepareJob(markersOff);
    assertEquals("DIRECT", merged.getString(MARKERS), "with out-of-line markers off, the markers are the job's");
    assertFalse(merged.containsKey(ROLLBACK));

    TypedProperties own = mode("blob");
    own.setProperty(CODEC, "zstd");
    own.setProperty(BLOOM, "true");
    TypedProperties kept = contract.prepareJob(own);
    assertEquals("zstd", kept.getString(CODEC), "the codec is a default");
    assertEquals("true", kept.getString(BLOOM), "the bloom filter is a default");
  }

  @Test
  void textOnlyEmbeddingKeepsDirectMarkers() {
    for (String extensions : new String[] {"pdf", "txt", "pdf,txt"}) {
      TypedProperties props = mode("embedding");
      props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), extensions);
      props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
          org.apache.hudi.utilities.unstructured.embedding.prep.TestPreprocessingIterator.PipeSplitPreprocessor.class.getName());
      props.setProperty(MARKERS, "DIRECT");
      TypedProperties merged = new UnstructuredSourceContract(props).prepareJob(props);
      assertEquals("DIRECT", merged.getString(MARKERS));
      assertFalse(merged.containsKey(ROLLBACK));
    }
  }

  @Test
  void inputStoringEmbeddingStillRequiresTimelineServerMarkers() {
    for (String extensions : new String[] {"jpg", "mp4", "pdf,jpg", ""}) {
      TypedProperties props = mode("embedding");
      if (!extensions.isEmpty()) {
        props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), extensions);
      }
      props.setProperty(MARKERS, "DIRECT");
      assertTrue(refusal(new UnstructuredSourceContract(props), props).contains("TIMELINE_SERVER_BASED"), extensions);
    }
  }

  @Test
  void goldenSettingsEmbedding() {
    Map<String, String> embedding = new TreeMap<>();
    embedding.put("hoodie.table.blob.layout.version", "1");
    embedding.put("hoodie.table.unstructured.kind", "embedding");
    embedding.put("hoodie.table.unstructured.schema.version", "1");
    embedding.put("hoodie.table.unstructured.key.encoding.version", "1");
    embedding.put(RECORD_KEY, EMBEDDING_KEY);
    embedding.put(MARKERS, "TIMELINE_SERVER_BASED");
    embedding.put(ROLLBACK, "true");
    embedding.put(INJECTOR, EmbeddingParquetConfigInjector.class.getName());
    UnstructuredSourceContract contract = contract("embedding");
    assertEquals(embedding, settingsOfAnEmptyJob(contract), "no codec or bloom filter");
    assertKeyGenerators(contract);
    assertEquals(BLOB_KEY, String.join(",", BlobTableSchema.KEY_COLUMNS));
    assertEquals(EMBEDDING_KEY, String.join(",", EmbeddingTableSchema.KEY_COLUMNS));
  }

  /** The complex or the non-partitioned key generator, named or inferred; any other is refused, naming the source and its mode. */
  private static void assertKeyGenerators(UnstructuredSourceContract contract) {
    for (String accepted : new String[] {ComplexKeyGenerator.class.getName(), NonpartitionedKeyGenerator.class.getName()}) {
      TypedProperties props = mode(contract.mode().configValue());
      props.setProperty(KEY_GENERATOR, accepted);
      contract.prepareJob(props);
    }
    TypedProperties partitioned = mode(contract.mode().configValue());
    partitioned.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "source_file_extension");
    contract.prepareJob(partitioned);
    for (String refused : new String[] {SimpleKeyGenerator.class.getName(), " " + ComplexKeyGenerator.class.getName() + " "}) {
      TypedProperties props = mode(contract.mode().configValue());
      props.setProperty(KEY_GENERATOR, refused);
      assertEquals(UnstructuredDataSource.name(contract.mode()) + " supports only " + ComplexKeyGenerator.class.getName() + " or "
          + NonpartitionedKeyGenerator.class.getName() + ", configured: " + refused, refusal(contract, props));
    }
  }

  /** A job that sets every required setting itself, to the same value, keeps its properties: the merge changes none of them. */
  @Test
  void jobThatSetsEveryRequiredKeyToTheSameValueStarts() {
    for (String mode : new String[] {"blob", "embedding"}) {
      TypedProperties props = mode(mode);
      props.setProperty(RECORD_KEY, "blob".equals(mode) ? BLOB_KEY : EMBEDDING_KEY);
      props.setProperty(KEY_GENERATOR, ComplexKeyGenerator.class.getName());
      props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "source_file_extension");
      props.setProperty(INJECTOR, "blob".equals(mode) ? BlobParquetConfigInjector.class.getName()
          : EmbeddingParquetConfigInjector.class.getName());
      props.setProperty(MARKERS, "TIMELINE_SERVER_BASED");
      props.setProperty(ROLLBACK, "true");
      if ("blob".equals(mode)) {
        props.setProperty(CODEC, "uncompressed");
        props.setProperty(BLOOM, "false");
      }
      props.putAll(new UnstructuredSourceContract(props).tableProperties());
      TypedProperties merged = new UnstructuredSourceContract(props).prepareJob(props);
      assertEquals(asMap(props), asMap(merged), "mode " + mode);
    }
  }

  /**
   * The job's props-only checks run with the merge, at the top of the streamer, before the table is created: the same refusals the
   * source makes as it is built, from one copy of the checks.
   */
  @Test
  void jobChecksRunWithTheMerge() {
    String[][] cases = {
        {HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "6", "UnstructuredDataSource (mode blob) refuses " + HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=6"},
        {EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY,
            "UnstructuredDataSource (mode blob) refuses hoodie.unstructured.embedding.mode=prep_only"},
    };
    for (String[] c : cases) {
      TypedProperties props = mode("blob");
      props.setProperty(c[0], c[1]);
      Exception refused = assertThrows(RuntimeException.class, () -> prepareJob(props), c[0]);
      assertTrue(refused.getMessage().contains(c[2]), refused.getMessage());
    }
    TypedProperties gcsWithS3Prefix = mode("blob");
    gcsWithS3Prefix.setProperty(UnstructuredSourceConfig.CLOUD.key(), "gcs");
    gcsWithS3Prefix.setProperty("hoodie.streamer.source.s3incr.fs.prefix", "s3a");
    assertTrue(assertThrows(IllegalArgumentException.class, () -> prepareJob(gcsWithS3Prefix)).getMessage()
        .startsWith("UnstructuredDataSource (mode blob) refuses hoodie.streamer.source.s3incr.fs.prefix"));
  }

  @Test
  void refusalsNameUnstructuredDataSourceAndMode() {
    TypedProperties injector = new TypedProperties();
    injector.setProperty(INJECTOR, "com.example.JobInjector");
    assertEquals("UnstructuredDataSource (mode embedding) requires " + INJECTOR + "=" + EmbeddingParquetConfigInjector.class.getName()
        + " (a job's own injector goes under " + EmbeddingParquetConfigInjector.DELEGATE_CLASS + ", and runs first), configured: "
        + "com.example.JobInjector", refusal(contract("embedding"), injector));
    TypedProperties blobKey = new TypedProperties();
    blobKey.setProperty(RECORD_KEY, BLOB_KEY);
    assertEquals("UnstructuredDataSource (mode embedding) requires " + RECORD_KEY + "=" + EMBEDDING_KEY + " (these fields, in this order),"
        + " configured: " + BLOB_KEY, refusal(contract("embedding"), blobKey));

    HoodieTableConfig blobTable = new HoodieTableConfig();
    blobTable.setValue(HoodieTableConfig.RECORDKEY_FIELDS, BLOB_KEY);
    UnstructuredMode embeddingMode = contract("embedding").mode();
    IllegalArgumentException table = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredDataSource.validate(embeddingMode, "file:/tables/blob", blobTable));
    assertEquals("UnstructuredDataSource (mode embedding) requires the table's " + HoodieTableConfig.RECORDKEY_FIELDS.key() + "="
        + EMBEDDING_KEY + ": the table at file:/tables/blob stores " + BLOB_KEY + "; write this job to a new table", table.getMessage());

    // the contract is built from the mode alone: an unset or unknown mode is refused as the contract is built
    TypedProperties noMode = new TypedProperties();
    assertTrue(assertThrows(IllegalArgumentException.class, () -> new UnstructuredSourceContract(noMode)).getMessage()
        .startsWith("UnstructuredDataSource requires " + UnstructuredSourceConfig.MODE.key() + ", one of: "));
    String sourceClass = UnstructuredDataSource.class.getName();
    TypedProperties videoMode = mode("video");
    assertThrows(IllegalArgumentException.class, () -> SourceContracts.of(sourceClass, videoMode),
        "the contract constructor's refusal reaches the caller as thrown");
  }

  @Test
  void preparationCanonicalizesRequiredValuesWithoutMutatingTheJob() {
    TypedProperties props = mode("blob");
    props.setProperty(RECORD_KEY, " " + BLOB_KEY.replace(",", " , ") + " ");
    props.setProperty(INJECTOR, " " + BlobParquetConfigInjector.class.getName() + " ");
    props.setProperty(CODEC, "zstd");
    TypedProperties before = TypedProperties.copy(props);
    TypedProperties merged = contract("blob").prepareJob(props);
    assertEquals(BLOB_KEY, merged.getString(RECORD_KEY));
    assertEquals(BlobParquetConfigInjector.class.getName(), merged.getString(INJECTOR));
    assertEquals("zstd", merged.getString(CODEC));
    assertEquals(before, props);
  }

  @Test
  void blankSettingsAreFilledAndUnrelatedSettingsAreKept() {
    TypedProperties props = mode("blob");
    props.setProperty(RECORD_KEY, " ");
    props.setProperty(CODEC, " ");
    props.setProperty("user.setting", "kept");
    TypedProperties prepared = contract("blob").prepareJob(props);
    assertEquals(BLOB_KEY, prepared.getString(RECORD_KEY));
    assertEquals("uncompressed", prepared.getString(CODEC));
    assertEquals("kept", prepared.getString("user.setting"));
  }

}
