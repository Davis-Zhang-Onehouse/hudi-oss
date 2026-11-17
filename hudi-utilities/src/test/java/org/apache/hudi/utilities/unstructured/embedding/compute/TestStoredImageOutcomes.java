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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.image.PreparedImage;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Embedding-compute outcomes of image rows read from their STORED input ({@code prepared_image}) by a multimodal client, through
 * {@link MicroBatchEmbeddingIterator}: a readable stored input is decoded, resized and embedded (image -> resized -> embedding);
 * a row whose {@code prepared_image} is NULL fails the step (prep never writes an image row without its stored input, so the
 * table's invariant is broken, not the row's input); a stored input whose file is confirmed gone is PERMANENT for its row
 * while its batch-mates embed; a stored input whose storage read fails transiently is RETRYABLE, retried in the step and,
 * still failing after {@code compute.max.attempts} tries, fails the task. Without Spark: the iterator over hand-built rows in
 * the table schema.
 */
public class TestStoredImageOutcomes {

  private static final String MODEL = "m1";
  private static final int DIM = 4;
  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType REFERENCE = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
  private static final String EMBEDDED = "EMBEDDED";
  private static final String SKIPPED = "SKIPPED ";
  private static final String PENDING = "PENDING";

  @TempDir
  Path base;

  @Test
  public void storedImageRowIsEmbeddedFromItsPreparedImageAndANullStoredInputFailsTheStep() throws Exception {
    TypedProperties props = props();
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 1200, 900);
    Path file = base.resolve("a.png");
    Files.write(file, png);
    ComputeCounters counters = counters();
    List<Row> out = drain(config, props, schema, counters, storedInputs(), row(schema, "a0", inline(png)),
        row(schema, "a1", reference(file)));
    assertEquals(2, out.size());
    PreparedImage expected = new ImagePreparer(config.imagePreprocessing()).prepare(ImageFixtures.source(png));
    assertEquals(512, expected.getWidth(), "the model input is the resized image");
    float[] vector = DummyMultimodalEmbeddingClient.vectorForImage(expected.getEncoded(), DIM);
    for (Row row : out) {
      assertEquals(EMBEDDED, outcome(row, schema, config));
      assertArrayEquals(vector, (float[]) row.get(schema.fieldIndex(config.model(MODEL).getColumn())),
          "inline and referenced stored inputs of the same image give the same vector");
    }
    assertEquals(2L, counters.getRowsEmbedded().value());

    // NULL prepared_image: the step fails before any call, no row is written as an error
    ComputeCounters nullCounters = counters();
    EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
        () -> drain(config, props, schema, nullCounters, storedInputs(), row(schema, "a0", inline(png)), row(schema, "b0", null)));
    assertTrue(e.getMessage().startsWith("EMBED-COMPUTE-NO-STORED-IMAGE"), e.getMessage());
    assertTrue(e.getMessage().contains("b0"), e.getMessage());
    assertEquals(0L, nullCounters.getRowsPermanent().value());
    assertEquals(0L, nullCounters.getBatchesOk().value() + nullCounters.getBatchesFailed().value(), "no client call");
  }

  /**
   * A stored input whose file is gone (the source file was deleted after prep) is the row's own fault, the same on every read:
   * PERMANENT, its error naming the row, while the readable batch-mate and a refused image's batch-mate embed in the same call.
   */
  @Test
  public void goneStoredFileAndUndecodableImageArePermanentForTheirRowOnly() throws Exception {
    TypedProperties props = props();
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    ComputeCounters counters = counters();
    Map<String, String> outcomes = outcomes(schema, config, drain(config, props, schema, counters, storedInputs(),
        row(schema, "a0", inline(ImageFixtures.image("png", 40, 30))),
        row(schema, "gone", reference(base.resolve("deleted.png"))),
        row(schema, "garbage", inline(ImageFixtures.garbage()))));
    assertEquals(EMBEDDED, outcomes.get("a0"));
    assertTrue(outcomes.get("gone").startsWith(SKIPPED + "stored input of source_path_sha=gone source_modification_time=1000 "
        + "source_file_size=1 modality=image chunk_id=0 cannot be read: FileNotFoundException"), outcomes.get("gone"));
    assertTrue(outcomes.get("garbage").startsWith(SKIPPED), outcomes.get("garbage"));
    assertEquals(1L, counters.getBatchesOk().value(), "one call for the whole batch");
    assertEquals(0L, counters.getRowsRetried().value());
    assertEquals(2L, counters.getRowsPermanent().value());
  }

  /**
   * A stored input whose storage read fails transiently is RETRYABLE: tried again alone until its {@code compute.max.attempts}
   * tries are spent, then the task fails with {@link EmbeddingComputeTransientException}. No row is ever yielded pending, nor
   * skipped for good.
   */
  @Test
  public void storedInputWhoseStorageFailsIsRetriedAndThenFailsTheTask() throws Exception {
    TypedProperties props = props();
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    Path file = base.resolve("flaky.png");
    Files.write(file, ImageFixtures.image("png", 40, 30));
    ComputeCounters counters = counters();
    List<Row> yielded = new ArrayList<>();
    EmbeddingComputeTransientException thrown = assertThrows(EmbeddingComputeTransientException.class,
        () -> yielded.addAll(drain(config, props, schema, counters, new FlakyStoredInputs(),
            row(schema, "a0", inline(ImageFixtures.image("png", 40, 30))), row(schema, "d0", reference(file)))));
    assertTrue(thrown.getMessage().contains("EMBED-COMPUTE-RETRIES-EXHAUSTED") && thrown.getMessage().contains("first source_path_sha=d0")
        && thrown.getMessage().contains("after " + EmbeddingTableConfig.DEFAULT_COMPUTE_MAX_ATTEMPTS + " tries")
        && thrown.getMessage().contains(FlakyStoredInputs.FAILURE), thrown.getMessage());
    assertEquals(Collections.emptyList(), yielded, "no row of the failing batch is yielded");
    assertEquals(1L, counters.getBatchesOk().value(), "the batch call answered d0 RETRYABLE, a0 OK");
    assertEquals(EmbeddingTableConfig.DEFAULT_COMPUTE_MAX_ATTEMPTS - 1L, counters.getRowsRetried().value(), "d0 alone, until its "
        + "tries were spent");
    assertEquals(0L, counters.getRowsEmbedded().value() + counters.getRowsPermanent().value(),
        "no outcome is counted for a batch that failed the task");
  }

  /**
   * A platform failure a client raises inside its call (a broken prep/compute invariant) is not retried and not reported as a
   * transient: the iterator rethrows it at once, as it does a configuration rejection, so the compute commit fails.
   */
  @Test
  public void platformFailureOfTheClientIsNotRetried() throws Exception {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        PlatformFailingClient.class.getName());
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    ComputeCounters counters = counters();
    EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
        () -> drain(config, props, schema, counters, storedInputs(), row(schema, "a0", inline(png)), row(schema, "b0", inline(png))));
    assertEquals(PlatformFailingClient.MESSAGE, e.getMessage());
    assertEquals(0L, counters.getRowsRetried().value(), "a platform failure is never retried");
    assertEquals(0L, counters.getRowsPermanent().value());
  }

  /**
   * The same platform failure raised on the single-input retry (the batch call failed generically, so each input is tried
   * alone) is rethrown at once too: the run fails with it after that one retry, never as a transient.
   */
  @Test
  public void platformFailureOnTheSingleInputRetryIsNotRetriedFurther() throws Exception {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        PlatformFailingAloneClient.class.getName());
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    ComputeCounters counters = counters();
    EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
        () -> drain(config, props, schema, counters, storedInputs(), row(schema, "a0", inline(png)), row(schema, "b0", inline(png))));
    assertEquals(PlatformFailingClient.MESSAGE, e.getMessage());
    assertEquals(1L, counters.getRowsRetried().value(), "only the first input's single retry ran");
  }

  /** A multimodal client whose batch call fails generically and whose single-input call fails on a broken invariant. */
  public static final class PlatformFailingAloneClient extends PlatformFailingClient {
    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      if (inputs.size() > 1) {
        throw new IllegalStateException("batch call failed (simulated)");
      }
      return super.embed(inputs);
    }
  }

  /** A multimodal client whose every call fails on a broken invariant. */
  public static class PlatformFailingClient implements MultimodalEmbeddingClient {
    static final String MESSAGE = "image input without a prepared image (model " + MODEL + ")";
    private EmbeddingModelSpec spec;

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      this.spec = spec;
    }

    @Override
    public EmbeddingModelSpec spec() {
      return spec;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      throw new EmbeddingComputePlatformException(MESSAGE);
    }
  }

  /** Stored inputs whose out-of-line opens fail in storage on every try (a connection reset); inline ones read as stored. */
  private static final class FlakyStoredInputs extends BlobBytesSource {
    static final String FAILURE = "Connection reset (simulated)";

    FlakyStoredInputs() {
      super(HadoopFSUtils.getStorageConf(new Configuration()));
    }

    @Override
    public SeekableBlob openSeekable(Row blob) throws IOException {
      if (HoodieSchema.Blob.OUT_OF_LINE.equals(blob.getString(0))) {
        throw new IOException(FAILURE);
      }
      return super.openSeekable(blob);
    }
  }

  private TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), MODEL);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, String.valueOf(DIM));
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX,
        DummyMultimodalEmbeddingClient.PROVIDER);
    props.setProperty(EmbeddingTableConfig.COMPUTE_RETRY_BACKOFF_MS.key(), "0");
    return props;
  }

  private static StructType schema(EmbeddingTableConfig config) {
    return EmbeddingTableSchema.build(config).add(ComputeRound.errorWorkingField());
  }

  private static BlobBytesSource storedInputs() {
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
  }

  private static List<Row> drain(EmbeddingTableConfig config, TypedProperties props, StructType schema, ComputeCounters counters,
                                 BlobBytesSource storedInputs, Row... rows) {
    List<Row> out = new ArrayList<>();
    try (MicroBatchEmbeddingIterator it = new MicroBatchEmbeddingIterator(Arrays.asList(rows).iterator(), config.model(MODEL),
        props, schema, 4, counters, storedInputs)) {
      it.forEachRemaining(out::add);
    }
    return out;
  }

  private static ComputeCounters counters() {
    return new ComputeCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator());
  }

  /** An inline stored input: the bytes as the source row carried them. */
  private static Row inline(byte[] bytes) {
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, bytes, null}, BLOB);
  }

  /** An out-of-line stored input: the unmanaged reference to the source file, with no range, as the source row carries it. */
  private static Row reference(Path file) {
    Row reference = new GenericRowWithSchema(new Object[] {file.toUri().toString(), null, null, false}, REFERENCE);
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB);
  }

  private static Map<String, String> outcomes(StructType schema, EmbeddingTableConfig config, List<Row> rows) {
    Map<String, String> outcomes = new TreeMap<>();
    for (Row row : rows) {
      outcomes.put(row.getString(schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)), outcome(row, schema, config));
    }
    return outcomes;
  }

  /** A processed row's outcome: EMBEDDED (vector), SKIPPED with its error, or PENDING (neither; the iterator never yields it). */
  private static String outcome(Row out, StructType schema, EmbeddingTableConfig config) {
    int vector = schema.fieldIndex(config.model(MODEL).getColumn());
    int error = schema.fieldIndex(ComputeRound.ERROR_WORKING_COLUMN);
    return !out.isNullAt(vector) ? EMBEDDED : !out.isNullAt(error) ? SKIPPED + out.getString(error) : PENDING;
  }

  /** An image row of the table schema, as compute reads it from the embedding table. */
  private static Row row(StructType schema, String sourcePathSha, Row preparedImage) {
    Object[] values = new Object[schema.size()];
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)] = sourcePathSha;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME)] = 1000L;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_SIZE)] = 1L;
    values[schema.fieldIndex(EmbeddingTableSchema.CHUNK_ID)] = 0L;
    values[schema.fieldIndex(EmbeddingTableSchema.MODALITY)] = EmbeddingTableSchema.MODALITY_IMAGE;
    values[schema.fieldIndex(EmbeddingTableSchema.PREPARED_IMAGE)] = preparedImage;
    return new GenericRowWithSchema(values, schema);
  }
}
