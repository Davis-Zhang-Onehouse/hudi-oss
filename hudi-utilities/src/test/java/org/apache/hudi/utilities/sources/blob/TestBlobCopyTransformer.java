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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.streamer.ErrorTableUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The real eager transformer, including ordinary corrupt-row routing and output cache lifecycle. */
@Tag("functional")
public class TestBlobCopyTransformer extends UtilitiesTestBase {
  private static final String INSTANT = "20261023120000000";
  private static final String CORRUPT_RECORD = BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;

  @TempDir
  Path temp;

  private SparkRDDWriteClient<?> markerClient;

  @BeforeAll
  public static void startServices() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void stopServices() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @BeforeEach
  public void initializeEmptyTable() throws IOException {
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
        .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("blob_table")
        .setRecordKeyFields(KEY_FIELDS).setPartitionFields(BlobTableSchema.SOURCE_FILE_EXTENSION)
        .initTable(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), temp.resolve("table").toUri().toString());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(temp.resolve("table").toUri().toString())
        .withEmbeddedTimelineServerEnabled(true).withEmbeddedTimelineServerPort(0)
        .withMarkersType("TIMELINE_SERVER_BASED").build();
    markerClient = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), config);
  }

  @AfterEach
  public void closeMarkerClient() throws IOException {
    if (markerClient != null) {
      markerClient.close();
    }
  }

  private TypedProperties properties(boolean errorTable) {
    TypedProperties props = new TypedProperties();
    props.putAll(markerClient.getConfig().getViewStorageConfig().getProps());
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), temp.resolve("table").toUri().toString());
    props.setProperty("hoodie.datasource.write.recordkey.field", KEY_FIELDS);
    props.setProperty("hoodie.datasource.write.partitionpath.field", BlobTableSchema.SOURCE_FILE_EXTENSION);
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), Boolean.toString(errorTable));
    return props;
  }

  private static final String KEY_FIELDS = BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + ","
      + BlobTableSchema.SOURCE_FILE_SIZE;

  /** The record key the complex key generator builds for {@code row} (a schemaless RowFactory row: columns by schema index). */
  private static String recordKey(Row row) {
    StructType schema = BlobTableSchema.schema();
    String sourcePath = row.getString(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_PATH));
    return BlobTableSchema.SOURCE_PATH_SHA + ":" + BlobFileNames.sourcePathSha(sourcePath) + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME
        + ":" + row.getLong(schema.fieldIndex(BlobTableSchema.SOURCE_MODIFICATION_TIME)) + "," + BlobTableSchema.SOURCE_FILE_SIZE
        + ":" + row.getLong(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_SIZE));
  }

  /** A blob row whose {@code source_file_name} is {@code tag}, so a test can tell its rows apart. */
  private static Row row(String source, String tag, Row blob, long length) {
    String sha = BlobFileNames.sourcePathSha(source);
    return RowFactory.create(sha, 1234L, source, tag, "pdf", length, "application/pdf", blob);
  }

  private Dataset<Row> rows(Row... rows) {
    return sparkSession.createDataFrame(Arrays.asList(rows), BlobTableSchema.schema());
  }

  @Test
  public void materializesManagedCopyAndPreservesControlsBeforeReturning() throws Exception {
    byte[] bytes = new byte[] {1, 2, 3, 4};
    Path source = temp.resolve("a.pdf");
    Files.write(source, bytes);
    String sourcePath = source.toUri().toString();
    Row managed = row(sourcePath, "managed", RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null,
        RowFactory.create("placeholder.pdf", 0L, 4L, true)), 4);
    Row inline = row("file:///inline.pdf", "inline", RowFactory.create(HoodieSchema.Blob.INLINE, bytes, null), 4);
    Row foreign = row("file:///unmanaged.pdf", "foreign", RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null,
        RowFactory.create("s3a://another/a.pdf", 1L, 4L, false)), 4);
    BlobCopyTransformer transformer = new BlobCopyTransformer();
    Dataset<Row> output = transformer.apply(jsc, sparkSession, rows(managed, inline, foreign), properties(true), INSTANT);
    int tagColumn = BlobTableSchema.schema().fieldIndex(BlobTableSchema.SOURCE_FILE_NAME);
    int blobColumn = BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB);
    Map<String, Row> byTag = output.collectAsList().stream().collect(Collectors.toMap(r -> r.getString(tagColumn), r -> r));
    String relative = byTag.get("managed").getStruct(blobColumn).getStruct(2).getString(0);
    assertTrue(relative.startsWith(".hoodie/.blob/pdf/"));
    assertTrue(relative.endsWith(".bin"));
    assertArrayEquals(bytes, Files.readAllBytes(temp.resolve("table").resolve(relative)), "apply already completed the copy");
    assertEquals(3, byTag.size());
    assertEquals(relative, byTag.get("managed").getStruct(blobColumn).getStruct(2).getString(0));
    assertArrayEquals(bytes, (byte[]) byTag.get("inline").getStruct(blobColumn).get(1));
    assertEquals(foreign.getStruct(blobColumn), byTag.get("foreign").getStruct(blobColumn));
    for (Row result : byTag.values()) {
      assertNull(result.getAs(CORRUPT_RECORD));
    }
    assertTrue(output.storageLevel().useMemory() || output.storageLevel().useDisk());
    transformer.apply(jsc, sparkSession, rows(), properties(true), "20261023120100000");
    assertEquals(StorageLevel.NONE(), output.storageLevel(), "the following round releases the prior output");
  }

  /** A copy buffer the copies cannot use is refused on the driver, by name, before any task copies a byte. */
  @Test
  public void unusableCopyBufferIsRefusedOnTheDriverBeforeAnyTask() throws Exception {
    Path source = temp.resolve("a.pdf");
    Files.write(source, new byte[] {1, 2, 3, 4});
    Row managed = row(source.toUri().toString(), "managed", RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null,
        RowFactory.create("placeholder.pdf", 0L, 4L, true)), 4);
    TypedProperties props = properties(true);
    props.setProperty(BlobTableConfig.COPY_BUFFER_BYTES.key(), "64k");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> new BlobCopyTransformer().apply(jsc, sparkSession, rows(managed), props, INSTANT));
    assertTrue(refused.getMessage().contains(BlobTableConfig.COPY_BUFFER_BYTES.key()), refused.getMessage());
    assertFalse(Files.exists(temp.resolve("table").resolve(".hoodie/.blob")), "no task copied a byte");
  }

  @Test
  public void contentFaultKeepsJsonIdentityAndOnePreAddedCorruptColumn() throws Exception {
    Row missingBytes = row("file:///gone.pdf", "gone", null, 10);
    Row inline = row("file:///inline.pdf", "inline", RowFactory.create(HoodieSchema.Blob.INLINE, new byte[] {9}, null), 1);
    Dataset<Row> input = rows(missingBytes, inline).withColumn(CORRUPT_RECORD,
        functions.when(functions.col(BlobTableSchema.SOURCE_FILE_NAME).equalTo("inline"), functions.lit("earlier-transformer-error")));
    BlobCopyTransformer transformer = new BlobCopyTransformer();
    Dataset<Row> output = transformer.apply(jsc, sparkSession, input, properties(true), INSTANT);
    assertEquals(1, Collections.frequency(Arrays.asList(output.columns()), CORRUPT_RECORD));
    List<Row> result = output.orderBy(BlobTableSchema.SOURCE_FILE_NAME).collectAsList();
    JsonNode identity = new ObjectMapper().readTree(result.get(0).getAs(CORRUPT_RECORD).toString());
    assertEquals(recordKey(missingBytes), identity.get("record_key").asText());
    assertEquals("pdf", identity.get("partition_path").asText());
    assertTrue(identity.get("error").asText().contains(BlobCopyFailure.class.getName()));
    assertTrue(identity.get("error").asText().contains("read:"));
    assertEquals("earlier-transformer-error", result.get(1).getAs(CORRUPT_RECORD));
    transformer.apply(jsc, sparkSession, rows(), properties(true), "20261023120100000");
  }

  @Test
  public void contentFaultKeepsJsonIdentityWithUntypedPreAddedCorruptColumn() throws Exception {
    Row missingBytes = row("file:///gone.pdf", "gone", null, 10);
    Dataset<Row> input = ErrorTableUtils.addNullValueErrorTableCorruptRecordColumn(rows(missingBytes));
    assertEquals(DataTypes.NullType, input.schema().apply(CORRUPT_RECORD).dataType(), "the actual chain pre-adds an untyped null");
    BlobCopyTransformer transformer = new BlobCopyTransformer();
    try {
      Dataset<Row> output = transformer.apply(jsc, sparkSession, input, properties(true), INSTANT);
      assertEquals(DataTypes.StringType, output.schema().apply(CORRUPT_RECORD).dataType(), "content JSON requires a string field");
      assertEquals(1, Collections.frequency(Arrays.asList(output.columns()), CORRUPT_RECORD));
      List<Row> result = output.collectAsList();
      assertEquals(1, result.size());
      JsonNode identity = new ObjectMapper().readTree(result.get(0).getAs(CORRUPT_RECORD).toString());
      assertEquals(recordKey(missingBytes), identity.get("record_key").asText());
      assertEquals("pdf", identity.get("partition_path").asText());
      assertTrue(identity.get("error").asText().contains(BlobCopyFailure.class.getName()));
      assertTrue(identity.get("error").asText().contains("read:"));
    } finally {
      transformer.apply(jsc, sparkSession, rows(), properties(true), "20261023120100000");
    }
  }

  @Test
  public void contentFaultWithoutErrorWriterThrowsInsideApply() {
    BlobCopyTransformer transformer = new BlobCopyTransformer();
    RuntimeException failure = assertThrows(RuntimeException.class,
        () -> transformer.apply(jsc, sparkSession, rows(row("file:///gone.pdf", "gone", null, 10)), properties(false), INSTANT));
    assertTrue(failure.getMessage().contains("without an error table"), failure.getMessage());
  }

  @Test
  public void successfulRowsWithoutErrorWriterKeepSourceSchema() {
    Row inline = row("file:///inline.pdf", "inline", RowFactory.create(HoodieSchema.Blob.INLINE, new byte[] {9}, null), 1);
    BlobCopyTransformer transformer = new BlobCopyTransformer();
    Dataset<Row> output = transformer.apply(jsc, sparkSession, rows(inline), properties(false), INSTANT);
    assertEquals(BlobTableSchema.schema(), output.schema());
    assertEquals(Collections.singletonList(inline), output.collectAsList());
    transformer.apply(jsc, sparkSession, rows(), properties(false), "20261023120100000");
  }
}
