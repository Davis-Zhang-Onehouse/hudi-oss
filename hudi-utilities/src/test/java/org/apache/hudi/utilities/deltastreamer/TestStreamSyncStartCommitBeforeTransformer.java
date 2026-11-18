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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.validator.SparkPreCommitValidator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.unstructured.storage.ManagedBlobPaths;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A transformer chain with an {@link InstantAwareTransformer}: once the source returned data StreamSync starts the commit
 * (REQUESTED), moves it to INFLIGHT right before the first {@link InstantAwareTransformer} of the chain, which receives the
 * INFLIGHT instant and writes marked files under {@code .hoodie/.blob/} (markers after inflight, the normal protocol); the write of
 * the round takes the instant that is already INFLIGHT and overwrites its inflight file with the workload profile; the commit
 * keeps the marked files its rows reference; a transformer failure or a crash leaves an INFLIGHT instant that the standard
 * marker-based rollback removes with its files; and the heartbeat of the instant survives the re-initialisation of the write
 * client. A chain without one runs as stock: the transformer runs before any instant. There is no option selecting either.
 */
@Tag("functional")
public class TestStreamSyncStartCommitBeforeTransformer extends HoodieDeltaStreamerTestBase {

  /** Property naming the target base path, so a test transformer can look at the table. */
  static final String TARGET_BASE_PATH = "test.instant.aware.target.base.path";
  /** The file a {@link BlobWritingTransformer} writes in the folder of its instant. */
  private static final java.util.Map<String, Path> PAYLOADS = new java.util.concurrent.ConcurrentHashMap<>();
  private static final java.util.Map<String, Path> PARTITIONED_PAYLOADS = new java.util.concurrent.ConcurrentHashMap<>();
  /** A partition of the test data, under which a {@link FailingAfterWriteTransformer} also writes its payload. */
  static final String PARTITION = "2016/03/15";

  /** One entry per call: the instant passed, or {@code null} for a call through the plain {@code apply}. */
  private static final List<String> SEEN_INSTANTS = new CopyOnWriteArrayList<>();
  /** One entry per call: the pending (requested or inflight) instants of the target table at the time of the call. */
  private static final List<List<HoodieInstant>> PENDING_AT_APPLY = new CopyOnWriteArrayList<>();
  private static final List<String> COMMIT_EVENTS = new CopyOnWriteArrayList<>();
  private static final List<String> TRANSFORMER_COMMITS = new CopyOnWriteArrayList<>();
  private static final List<List<String>> COMPLETED_AT_CALLBACK = new CopyOnWriteArrayList<>();
  private static final AtomicLong RECORDS_AT_FAILED_COMMIT = new AtomicLong(-1);

  /** The ordinary trip source declares the managed layout its test transformers write at table creation. */
  public static class ManagedPayloadSource extends TestDataSource {
    public ManagedPayloadSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark, SchemaProvider schemaProvider) {
      super(props, jsc, spark, schemaProvider);
    }
  }

  /** Records the source's existing commit callback so the transformer hook must follow it. */
  public static class CommitRecordingSource extends ManagedPayloadSource {
    public CommitRecordingSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark, SchemaProvider schemaProvider) {
      super(props, jsc, spark, schemaProvider);
    }

    @Override
    public void onCommit(String checkpoint) {
      super.onCommit(checkpoint);
      COMMIT_EVENTS.add("source");
    }
  }

  /** Records the completed timeline at callback time, before any later test-side observation. */
  public static class CommitRecordingTransformer extends BlobWritingTransformer {
    // Intentionally compiles before the interface introduces the commit callback.
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties props, String instant) {
      TRANSFORMER_COMMITS.add(instant);
      COMPLETED_AT_CALLBACK.add(completedCommits(props.getString(TARGET_BASE_PATH)));
      COMMIT_EVENTS.add("transformer");
    }
  }

  /** {@link CommitRecordingTransformer} without an instant: a plain transformer, so its chain runs as stock. */
  public static class PlainCommitRecordingTransformer implements org.apache.hudi.utilities.transform.Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      BlobWritingTransformer.record(properties, null);
      return rowDataset;
    }

    @Override
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties props, String instant) {
      TRANSFORMER_COMMITS.add(instant);
      COMPLETED_AT_CALLBACK.add(completedCommits(props.getString(TARGET_BASE_PATH)));
      COMMIT_EVENTS.add("transformer");
    }
  }

  /** A plain transformer that records each call and the pending instants at that moment, and writes nothing. */
  public static class PlainRecordingTransformer implements org.apache.hudi.utilities.transform.Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      BlobWritingTransformer.record(properties, null);
      return rowDataset;
    }
  }

  /** An {@link InstantAwareTransformer} that records each call and writes nothing. */
  public static class PassThroughInstantAwareTransformer extends PlainRecordingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      BlobWritingTransformer.record(properties, instantTime);
      return rowDataset;
    }
  }

  /** A plain transformer that records its call and the pending instants, then fails: before any instant-aware transformer. */
  public static class FailingPlainTransformer implements org.apache.hudi.utilities.transform.Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      BlobWritingTransformer.record(properties, null);
      throw new HoodieTransformException("injected plain transformer failure");
    }
  }

  public static class ThrowingCommitTransformer extends CommitRecordingTransformer {
    public void onCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties props, String instant) {
      super.onCommit(jsc, spark, props, instant);
      throw new IllegalStateException("injected transformer post-commit failure at " + instant);
    }
  }

  /** Produces an empty write with a changed checkpoint, so the commit client can skip it. */
  public static class EmptyCommitTransformer extends CommitRecordingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props, String instant) {
      return super.apply(jsc, spark, rows, props, instant).limit(0);
    }
  }

  /** Fails inside the actual commit call, after the round has written its records. */
  public static class RejectingCommitValidator<T, I, K, O extends HoodieData<WriteStatus>>
      extends SparkPreCommitValidator<T, I, K, O> {
    public RejectingCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext context, HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    public void validate(String instant, HoodieWriteMetadata<O> result, Dataset<Row> before, Dataset<Row> after) {
      RECORDS_AT_FAILED_COMMIT.set(result.getWriteStats().get().stream().mapToLong(HoodieWriteStat::getNumWrites).sum());
      throw new HoodieValidationException("injected commit failure after records were written");
    }

    @Override
    protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after, Set<String> partitions) {
      throw new HoodieValidationException("injected commit failure after records were written");
    }
  }

  @BeforeEach
  void clearCalls() {
    SEEN_INSTANTS.clear();
    PAYLOADS.clear();
    PARTITIONED_PAYLOADS.clear();
    PENDING_AT_APPLY.clear();
    COMMIT_EVENTS.clear();
    TRANSFORMER_COMMITS.clear();
    COMPLETED_AT_CALLBACK.clear();
    RECORDS_AT_FAILED_COMMIT.set(-1);
  }

  /** Records each call; given an instant, writes a fresh marked file. */
  public static class BlobWritingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      record(properties, null);
      return rowDataset;
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      record(properties, instantTime);
      try {
        String base = properties.getString(TARGET_BASE_PATH);
        org.apache.hudi.common.table.HoodieTableConfig.update(storage,
            new org.apache.hudi.storage.StoragePath(base, ".hoodie"),
            org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties.layoutOne());
        String relative = org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter.forTable(storage.getConf(), base, properties)
            .forInstant(storage, instantTime)
            .write("", out -> { });
        PAYLOADS.put(base + "#" + instantTime, new Path(base, relative));
      } catch (IOException e) {
        throw new HoodieTransformException("Cannot write the payload of " + instantTime, e);
      }
      return rowDataset;
    }

    static void record(TypedProperties properties, String instantTime) {
      SEEN_INSTANTS.add(instantTime == null ? "" : instantTime);
      String basePath = properties.getString(TARGET_BASE_PATH);
      try {
        PENDING_AT_APPLY.add(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
            ? HoodieTestUtils.createMetaClient(storage, basePath).getActiveTimeline().filterInflightsAndRequested().getInstants()
            : Collections.emptyList());
      } catch (IOException e) {
        throw new HoodieTransformException("Cannot read the timeline of " + basePath, e);
      }
    }
  }

  /** Writes its payload like {@link BlobWritingTransformer}, then dies with an error the streamer does not catch: a crash. */
  public static class CrashingAfterWriteTransformer extends BlobWritingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      super.apply(jsc, sparkSession, rowDataset, properties, instantTime);
      throw new SimulatedCrash("simulated crash after the transformer wrote at " + instantTime);
    }
  }

  /** An {@link Error}, so no rollback in the streamer runs: the round's instant stays as the crash left it. */
  static class SimulatedCrash extends Error {
    SimulatedCrash(String message) {
      super(message);
    }
  }

  /** Writes its payload like {@link BlobWritingTransformer}, then fails. */
  public static class FailingAfterWriteTransformer extends BlobWritingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      super.apply(jsc, sparkSession, rowDataset, properties, instantTime);
      try {
        String base = properties.getString(TARGET_BASE_PATH);
        org.apache.hudi.common.table.HoodieTableConfig.update(storage,
            new org.apache.hudi.storage.StoragePath(base, ".hoodie"),
            org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties.layoutOne());
        String relative = org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter.forTable(storage.getConf(), base, properties)
            .forInstant(storage, instantTime)
            .write(PARTITION, out -> { });
        PARTITIONED_PAYLOADS.put(base + "#" + instantTime, new Path(base, relative));
      } catch (IOException e) {
        throw new HoodieTransformException("Cannot write the partitioned payload of " + instantTime, e);
      }
      throw new HoodieTransformException("injected transformer failure at " + instantTime);
    }
  }

  /** A plain transformer whose rows sleep when they are evaluated, so the write runs longer than the heartbeat tolerance. */
  public static class SlowRowsTransformer extends TestHoodieDeltaStreamer.TripsWithDistanceTransformer {
    static final long SLEEP_MS = 5000;

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      Dataset<Row> withDistance = super.apply(jsc, sparkSession, rowDataset, properties);
      return sparkSession.createDataFrame(withDistance.javaRDD().mapPartitions(rows -> {
        Thread.sleep(SLEEP_MS);
        return rows;
      }), withDistance.schema());
    }
  }

  /** Armed by {@link TimeoutArmingTransformer}: the next target-schema read after it throws a source timeout, once. */
  private static final AtomicBoolean TIMEOUT_ARMED = new AtomicBoolean();

  /** Writes its payload like {@link BlobWritingTransformer}; on its first call it arms {@link TimeoutOnceSchemaProvider}. */
  public static class TimeoutArmingTransformer extends BlobWritingTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      Dataset<Row> rows = super.apply(jsc, sparkSession, rowDataset, properties, instantTime);
      if (SEEN_INSTANTS.size() == 1) {
        TIMEOUT_ARMED.set(true);
      }
      return rows;
    }
  }

  /** A file-based schema provider whose target-schema read throws a source timeout once, when armed. */
  public static class TimeoutOnceSchemaProvider extends FilebasedSchemaProvider {
    public TimeoutOnceSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getTargetHoodieSchema() {
      if (TIMEOUT_ARMED.compareAndSet(true, false)) {
        throw new HoodieSourceTimeoutException("injected source timeout after the transformer");
      }
      return super.getTargetHoodieSchema();
    }
  }

  static Path payloadPath(String basePath, String instantTime) {
    return PAYLOADS.get(basePath + "#" + instantTime);
  }

  static Path partitionedPayloadPath(String basePath, String instantTime) {
    return PARTITIONED_PAYLOADS.get(basePath + "#" + instantTime);
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, List<String> transformers) {
    return config(tableBasePath, transformers, WriteOperationType.INSERT);
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, List<String> transformers, WriteOperationType operation) {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, operation, transformers);
    cfg.sourceClassName = ManagedPayloadSource.class.getName();
    cfg.configs.add(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "=LAZY");
    cfg.configs.add(HoodieWriteConfig.MARKERS_TYPE.key() + "=TIMELINE_SERVER_BASED");
    cfg.configs.add(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key() + "=true");
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    return cfg;
  }

  private static List<String> transformers(Class<?>... classes) {
    return Arrays.stream(classes).map(Class::getName).collect(Collectors.toList());
  }

  private static HoodieDeltaStreamer.Config commitHookConfig(String tableBasePath, Class<?> transformer) {
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(transformer, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    cfg.sourceClassName = CommitRecordingSource.class.getName();
    return cfg;
  }

  private static HoodieTableMetaClient metaClient(String tableBasePath) {
    return HoodieTestUtils.createMetaClient(storage, tableBasePath);
  }

  private static List<String> completedCommits(String tableBasePath) {
    return metaClient(tableBasePath).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  private static String causes(Throwable e) {
    StringBuilder messages = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
      messages.append(t).append(" | ");
    }
    return messages.toString();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void transformerCommitCallbackRunsOnceAfterSourceAndDurableCommit(boolean earlyCommit) throws Exception {
    String tableBasePath = basePath + "/transformer_commit_callback_" + earlyCommit;
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath,
        earlyCommit ? CommitRecordingTransformer.class : PlainCommitRecordingTransformer.class);
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));

    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), "the round durably committed");
    assertEquals(commits, TRANSFORMER_COMMITS, "the transformer is notified exactly once of that commit");
    assertEquals(Collections.singletonList(commits), COMPLETED_AT_CALLBACK, "the instant was already completed at callback time");
    assertEquals(Arrays.asList("source", "transformer"), COMMIT_EVENTS, "Source.onCommit runs before the transformer callback");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  @Test
  void transformerCommitCallbackDoesNotRunWhenActualCommitFails() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_failed_commit";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, CommitRecordingTransformer.class);
    cfg.configs.add(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key() + "=" + RejectingCommitValidator.class.getName());
    Exception failure = assertThrows(Exception.class, () -> syncOnce(new HoodieDeltaStreamer(cfg, jsc)));

    assertTrue(causes(failure).contains("pre-commit validation failed"), causes(failure));
    assertTrue(RECORDS_AT_FAILED_COMMIT.get() > 0, "the validator rejected a commit after the actual write");
    assertEquals(1, SEEN_INSTANTS.size(), "the transformer returned before the commit failed");
    assertTrue(completedCommits(tableBasePath).isEmpty());
    assertTrue(TRANSFORMER_COMMITS.isEmpty(), "a failed commit cannot publish transformer state");
    assertTrue(COMMIT_EVENTS.isEmpty(), "neither source nor transformer receives a successful commit callback");
  }

  @Test
  void transformerCommitCallbackDoesNotRunForAnUnchangedSourceCheckpoint() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_no_new_data";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, CommitRecordingTransformer.class);
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));
    List<String> before = completedCommits(tableBasePath);
    TRANSFORMER_COMMITS.clear();
    COMMIT_EVENTS.clear();
    SEEN_INSTANTS.clear();
    cfg.sourceLimit = 0;
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));

    assertEquals(before, completedCommits(tableBasePath), "the empty source round adds no completed instant");
    assertTrue(SEEN_INSTANTS.isEmpty(), "the unchanged checkpoint skips transformation");
    assertTrue(TRANSFORMER_COMMITS.isEmpty());
    assertTrue(COMMIT_EVENTS.isEmpty());
  }

  @Test
  void transformerCommitCallbackDoesNotRunWhenAnEmptyWriteSkipsCommit() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_skipped_empty_commit";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, EmptyCommitTransformer.class);
    cfg.configs.add(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key() + "=false");
    syncOnce(new HoodieDeltaStreamer(cfg, jsc));

    assertEquals(1, SEEN_INSTANTS.size(), "the changed checkpoint reached the transformer and write path");
    assertTrue(completedCommits(tableBasePath).isEmpty(), "empty commit suppression leaves no durable instant");
    assertTrue(TRANSFORMER_COMMITS.isEmpty(), "a successful return for a skipped commit cannot publish transformer state");
  }

  @Test
  void throwingTransformerCommitCallbackFailsTheRoundWithoutRollingBackCompletedData() throws Exception {
    String tableBasePath = basePath + "/transformer_callback_throws_after_commit";
    HoodieDeltaStreamer.Config cfg = commitHookConfig(tableBasePath, ThrowingCommitTransformer.class);
    Exception failure = assertThrows(Exception.class, () -> syncOnce(new HoodieDeltaStreamer(cfg, jsc)));

    assertTrue(causes(failure).contains("injected transformer post-commit failure"), causes(failure));
    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), "the durable commit survives a callback failure");
    assertEquals(commits, TRANSFORMER_COMMITS);
    assertEquals(Collections.singletonList(commits), COMPLETED_AT_CALLBACK);
    assertEquals(Arrays.asList("source", "transformer"), COMMIT_EVENTS);
    assertEquals(0, metaClient(tableBasePath).getActiveTimeline().getRollbackTimeline().countInstants(),
        "completed data is not rolled back");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
    assertTrue(fs.exists(payloadPath(tableBasePath, commits.get(0))), "the committed managed payload survives too");
  }

  /**
   * A chain with no {@link InstantAwareTransformer} runs as stock: the transformer runs through the plain apply before any instant
   * exists, nothing is written under the meta folder, and the commit carries no restore refusal.
   */
  @Test
  void withoutInstantAwareTransformerTheChainRunsAsStock() throws Exception {
    String tableBasePath = basePath + "/early_commit_plain_chain";
    syncOnce(config(tableBasePath, transformers(PlainRecordingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class)));

    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "called through the plain apply, without an instant");
    assertEquals(Collections.singletonList(Collections.emptyList()), PENDING_AT_APPLY, "no instant was pending at apply");
    assertEquals(1, completedCommits(tableBasePath).size());
    assertFalse(fs.exists(new Path(tableBasePath, ManagedBlobPaths.FOLDER)), "nothing written under .hoodie/.blob");
    HoodieTimeline timeline = metaClient(tableBasePath).getActiveTimeline();
    assertFalse(timeline.readCommitMetadata(timeline.getCommitsTimeline().lastInstant().get()).getExtraMetadata()
        .containsKey(RestorePlanActionExecutor.RESTORE_UNSUPPORTED_REASON_KEY), "a stock round does not refuse restore");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * The instant is REQUESTED once the source returned data, and INFLIGHT right before the first {@link InstantAwareTransformer}
   * of the chain: a plain transformer before it sees the instant REQUESTED, the instant-aware one sees it INFLIGHT before it
   * writes its marked file, and the data files of the write come after the inflight file the write overwrote with its workload
   * profile.
   */
  @Test
  void requestedOnceTheSourceHasDataAndInflightRightBeforeTheFirstInstantAwareTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_requested_then_inflight";
    syncOnce(config(tableBasePath, transformers(PlainRecordingTransformer.class, BlobWritingTransformer.class,
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class), WriteOperationType.UPSERT));

    assertEquals(2, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertEquals("", SEEN_INSTANTS.get(0), "the plain transformer is called through the plain apply");
    String instant = SEEN_INSTANTS.get(1);
    assertEquals(1, PENDING_AT_APPLY.get(0).size(), PENDING_AT_APPLY.toString());
    assertEquals(instant, PENDING_AT_APPLY.get(0).get(0).requestedTime());
    assertEquals(HoodieInstant.State.REQUESTED, PENDING_AT_APPLY.get(0).get(0).getState(),
        "the instant is REQUESTED before the first instant-aware transformer");
    assertEquals(1, PENDING_AT_APPLY.get(1).size(), PENDING_AT_APPLY.toString());
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(1).get(0).getState(),
        "the instant is INFLIGHT when the instant-aware transformer writes");
    assertEquals(Collections.singletonList(instant), completedCommits(tableBasePath));
    assertTrue(fs.exists(payloadPath(tableBasePath, instant)), "the committed payload survives");

    // the payload was written while the instant was INFLIGHT (above); the data files of the write come after the payload
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    long payloadWritten = fs.getFileStatus(payloadPath(tableBasePath, instant)).getModificationTime();
    List<HoodieWriteStat> stats = metaClient.getActiveTimeline().readCommitMetadata(
        metaClient.getActiveTimeline().getCommitsTimeline().lastInstant().get()).getWriteStats();
    assertFalse(stats.isEmpty());
    for (HoodieWriteStat stat : stats) {
      assertTrue(fs.getFileStatus(new Path(tableBasePath, stat.getPath())).getModificationTime() >= payloadWritten,
          stat.getPath() + " is written after the payload");
    }
  }

  /**
   * A failure while the instant is still REQUESTED (a plain transformer before the first instant-aware one) rolls the REQUESTED
   * instant back with the stock rollback: no file was written for it, nothing stays pending, and its heartbeat is stopped.
   */
  @Test
  void instantAwareFailureWhileRequestedRollsBackTheRequestedInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_failure_requested";
    HoodieDeltaStreamer.Config cfg = withCleaningPolicy(config(tableBasePath, transformers(FailingPlainTransformer.class,
        BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class)), "LAZY");
    Exception e = assertThrows(Exception.class, () -> syncOnce(cfg));
    assertTrue(causes(e).contains("injected plain transformer failure"), causes(e));

    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "the instant-aware transformer never ran");
    assertEquals(1, PENDING_AT_APPLY.get(0).size(), PENDING_AT_APPLY.toString());
    assertEquals(HoodieInstant.State.REQUESTED, PENDING_AT_APPLY.get(0).get(0).getState());
    String instant = PENDING_AT_APPLY.get(0).get(0).requestedTime();
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "no pending or completed commit");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), rollbacks.toString());
    assertEquals(instant, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertTrue(PAYLOADS.isEmpty(), "nothing was written for the instant");
    assertFalse(fs.exists(new Path(HoodieTableMetaClient.getHeartbeatFolderPath(tableBasePath), instant)), "heartbeat stopped");
  }

  /**
   * With {@code --retry-last-pending-inline-clustering-job}, the streamer retries the table's pending clustering before it starts the
   * round's instant, so the clustering does not run against the round's own empty INFLIGHT instant (which, under
   * {@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution}, would refuse it in every round).
   */
  @Test
  void instantAwareRetriesThePendingClusteringBeforeStartingItsInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_retry_clustering";
    HoodieDeltaStreamer.Config cfg = withCleaningPolicy(config(tableBasePath, transformers(BlobWritingTransformer.class,
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class), WriteOperationType.BULK_INSERT), "LAZY");
    cfg.configs.add("hoodie.write.lock.conflict.resolution.strategy="
        + org.apache.hudi.client.transaction.PreferWriterConflictResolutionStrategy.class.getName());
    cfg.configs.add(HoodieWriteConfig.CLUSTERING_BLOCK_FOR_PENDING_INGESTION.key() + "=true");
    syncOnce(cfg);
    cfg.sourceLimit = 2000;
    syncOnce(cfg);
    String clustering;
    try (SparkRDDWriteClient<?> client = writeClient(tableBasePath)) {
      clustering = client.scheduleClustering(Option.empty()).get();
    }

    cfg.retryLastPendingInlineClusteringJob = true;
    cfg.configs.add("hoodie.clustering.inline=true");
    cfg.sourceLimit = 3000;
    syncOnce(cfg);

    HoodieTimeline timeline = metaClient(tableBasePath).getActiveTimeline();
    assertTrue(timeline.filterCompletedInstants().containsInstant(clustering), "the pending clustering was retried and completed");
    assertEquals(0, timeline.filterInflightsAndRequested().countInstants(), "nothing left pending");
    assertEquals(3, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertTrue(timeline.filterCompletedInstants().containsInstant(SEEN_INSTANTS.get(2)), "the round committed");
    String clusteringCompleted = timeline.filterCompletedInstants().getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(clustering)).findFirst().get().getCompletionTime();
    assertTrue(clusteringCompleted.compareTo(SEEN_INSTANTS.get(2)) < 0,
        "the clustering completed (" + clusteringCompleted + ") before the round's instant was started (" + SEEN_INSTANTS.get(2) + ")");
  }

  /**
   * Two rounds (an empty table, then one with a schema): each round's transformer receives the instant, already INFLIGHT, that
   * the round then commits and writes a marked file under its folder; the commit's marker reconciliation keeps that file, which
   * no write stat names; the plain transformer of the chain still runs.
   */
  @Test
  void instantAwarePassesTheInflightInstantToTheTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_flag_on";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(cfg);
    cfg.sourceLimit = 2000;
    syncOnce(cfg);

    assertEquals(2, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertNotEquals(SEEN_INSTANTS.get(0), SEEN_INSTANTS.get(1));
    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath), "each round committed the instant its transformer saw");
    for (int i = 0; i < 2; i++) {
      String instant = SEEN_INSTANTS.get(i);
      List<HoodieInstant> pending = PENDING_AT_APPLY.get(i);
      assertEquals(1, pending.size(), pending.toString());
      assertEquals(instant, pending.get(0).requestedTime());
      assertEquals(HoodieInstant.State.INFLIGHT, pending.get(0).getState(), "the instant is INFLIGHT at apply");
      assertTrue(fs.exists(payloadPath(tableBasePath, instant)),
          "the committed payload of " + instant + " survives the commit's marker reconciliation");
      assertFalse(fs.exists(new Path(metaClient(tableBasePath).getMarkerFolderPath(instant))), "the commit removed its markers");
    }
    Dataset<Row> table = sqlContext.read().format("hudi").load(tableBasePath);
    long rows = table.count();
    assertTrue(rows > SQL_SOURCE_NUM_RECORDS, "both rounds wrote rows: " + rows);
    assertEquals(rows, table.where("haversine_distance is not null").count(), "the plain transformer of the chain ran");
  }

  /**
   * The write path of each operation (the commit action executor for UPSERT, the bulk-insert helper for BULK_INSERT) takes the
   * instant that is already INFLIGHT, re-writing its inflight file instead of failing on it, and commits it with the payload.
   */
  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"UPSERT", "BULK_INSERT"})
  void instantAwareWriteTakesTheInflightInstant(WriteOperationType operation) throws Exception {
    String tableBasePath = basePath + "/early_commit_" + operation.name().toLowerCase();
    syncOnce(config(tableBasePath, transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class), operation));

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    String instant = SEEN_INSTANTS.get(0);
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(0).get(0).getState(), "the instant is INFLIGHT at apply");
    assertEquals(Collections.singletonList(instant), completedCommits(tableBasePath), "the write committed that instant");
    HoodieTimeline timeline = metaClient(tableBasePath).getActiveTimeline();
    assertEquals(operation, timeline.readCommitMetadata(timeline.getCommitsTimeline().lastInstant().get()).getOperationType());
    assertTrue(fs.exists(payloadPath(tableBasePath, instant)), "the committed payload survives the commit");
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(2, metaClient.getTimelineLayoutVersion().getVersion(), "timeline layout v2: the inflight file is write-once");
    if (operation == WriteOperationType.UPSERT) {
      // the write overwrote the inflight file the streamer created empty with its workload profile
      HoodieCommitMetadata profile = metaClient.getActiveTimeline().readCommitMetadata(
          metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant));
      assertEquals(WriteOperationType.UPSERT, profile.getOperationType(), "the inflight file holds the workload profile");
      assertFalse(profile.getPartitionToWriteStats().isEmpty(), "with the file groups the write touches");
    }
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * BULK_INSERT through the row writer (a ROW source, the streamer's row-writer switch on) builds its own write config; its
   * executor must also take the instant that is already INFLIGHT: otherwise its requested-to-inflight transition fails on the
   * existing inflight file and every round fails.
   */
  @Test
  void instantAwareRowWriterBulkInsertTakesTheInflightInstant() throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesEarlyCommit" + testNum;
    int records = 100;
    prepareParquetDFSFiles(records, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(true, true, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET, PARQUET_SOURCE_ROOT, false,
        "partition_path", "", false, true);
    String tableBasePath = basePath + "/early_commit_row_writer" + testNum;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT, ParquetDFSSource.class.getName(),
        transformers(PassThroughInstantAwareTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class),
        PROPS_FILENAME_TEST_PARQUET, false, true, 100000, false, null, null, "timestamp", null, false);
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    syncOnce(cfg);

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(0).get(0).getState(), "the instant is INFLIGHT at apply");

    List<String> commits = completedCommits(tableBasePath);
    assertEquals(1, commits.size(), commits.toString());
    assertEquals(0, metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().countInstants(), "nothing left pending");
    assertRecordCount(records, tableBasePath, sqlContext);
  }

  /**
   * Restore refuses to roll back a commit whose instant was handed to the transformer before the write: the files the
   * transformer wrote under it are not in its write stats, so a restore would leave them behind. Commits of a chain without an
   * instant-aware transformer are restored as before, and nothing changes when the restore is refused.
   */
  @Test
  void instantAwareRestoreRefusesToRollBackARoundStartedBeforeTheTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_restore";
    HoodieDeltaStreamer.Config on = config(tableBasePath, transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(on);
    String savepoint = completedCommits(tableBasePath).get(0);
    HoodieDeltaStreamer.Config off = config(tableBasePath, transformers(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    off.sourceLimit = 2000;
    syncOnce(off);
    assertEquals(2, completedCommits(tableBasePath).size());

    try (SparkRDDWriteClient<?> client = writeClient(tableBasePath)) {
      client.savepoint(savepoint, "test", "before the stock round");
      client.restoreToSavepoint(savepoint);
    }
    assertEquals(Collections.singletonList(savepoint), completedCommits(tableBasePath), "the stock round was restored away");

    on.sourceLimit = 3000;
    syncOnce(on);
    List<String> commits = completedCommits(tableBasePath);
    String startedBeforeTransformer = commits.get(commits.size() - 1);
    try (SparkRDDWriteClient<?> client = writeClient(tableBasePath)) {
      Exception refused = assertThrows(Exception.class, () -> client.restoreToSavepoint(savepoint));
      assertTrue(causes(refused).contains(startedBeforeTransformer) && causes(refused).contains(RestorePlanActionExecutor.RESTORE_UNSUPPORTED_REASON_KEY),
          causes(refused));
    }
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(Arrays.asList(savepoint, startedBeforeTransformer), completedCommits(tableBasePath), "a refused restore rolls nothing back");
    assertEquals(0, metaClient.getActiveTimeline().getRestoreTimeline().filterInflightsAndRequested().countInstants(),
        "a refused restore leaves no pending restore");
    assertTrue(fs.exists(payloadPath(tableBasePath, startedBeforeTransformer)), "the round's payload is still referenced");
  }

  private static SparkRDDWriteClient<?> writeClient(String tableBasePath) {
    return new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc),
        HoodieWriteConfig.newBuilder().withPath(tableBasePath).withEmbeddedTimelineServerEnabled(false).build());
  }


  /** A round with no new data starts no instant and does not call the transformer. */
  @Test
  void instantAwareNoNewDataStartsNoInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_no_new_data";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(cfg);
    List<HoodieInstant> before = metaClient(tableBasePath).getActiveTimeline().getInstants();
    // the test source returns no data and its previous checkpoint for a source limit of 0
    cfg.sourceLimit = 0;
    syncOnce(cfg);

    assertEquals(1, SEEN_INSTANTS.size(), "the transformer ran only in the round with data");
    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath));
    assertEquals(before, metaClient(tableBasePath).getActiveTimeline().getInstants(), "the round with no new data wrote no instant");
  }

  /**
   * A transformer failure rolls back the INFLIGHT instant, and the standard marker-based rollback removes its marked files, with
   * eager or lazy OCC cleaning. The marker directory and heartbeat are removed after rollback.
   */
  @ParameterizedTest
  @ValueSource(strings = {"EAGER", "LAZY"})
  void instantAwareTransformerFailureRollsBackTheInflightInstant(String cleaningPolicy) throws Exception {
    String tableBasePath = basePath + "/early_commit_failure";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(FailingAfterWriteTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    cfg.configs.removeIf(value -> value.startsWith(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "="));
    cfg.configs.add(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "=" + cleaningPolicy);
    if ("LAZY".equals(cleaningPolicy)) {
      cfg.configs.add("hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL");
      cfg.configs.add("hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    }
    Exception e = assertThrows(Exception.class, () -> syncOnce(cfg));
    assertTrue(causes(e).contains("injected transformer failure"), causes(e));

    assertEquals(1, SEEN_INSTANTS.size());
    String instant = SEEN_INSTANTS.get(0);
    assertEquals(HoodieInstant.State.INFLIGHT, PENDING_AT_APPLY.get(0).get(0).getState());
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().getCommitsTimeline().countInstants(), "no pending or completed commit");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), "the instant was rolled back explicitly");
    assertEquals(instant, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertFalse(fs.exists(payloadPath(tableBasePath, instant)), "the marked copy is removed by rollback");
    assertFalse(fs.exists(partitionedPayloadPath(tableBasePath, instant)), "and its marked copy under a partition");
    assertTrue(fs.exists(partitionedPayloadPath(tableBasePath, instant).getParent().getParent()),
        "the partition ancestors remain");
    assertFalse(fs.exists(new Path(metaClient.getMarkerFolderPath(instant))), "rollback removed its marker directory");
    assertFalse(fs.exists(new Path(HoodieTableMetaClient.getHeartbeatFolderPath(tableBasePath), instant)), "heartbeat stopped");
  }

  /**
   * A crash after the transformer wrote leaves the instant INFLIGHT with its marked files. The next streamer (a restart) rolls it
   * back with the standard rollback of failed writes, which removes those files, and its round then commits cleanly.
   */
  @ParameterizedTest
  @ValueSource(strings = {"EAGER", "LAZY"})
  void instantAwareCrashLeavesAnInflightInstantThatTheRestartRollsBack(String cleaningPolicy) throws Exception {
    String tableBasePath = basePath + "/early_commit_crash_" + cleaningPolicy.toLowerCase();
    // a first round, so the crashed instant is not the first instant of the table
    HoodieDeltaStreamer.Config first = config(tableBasePath, transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(withCleaningPolicy(first, cleaningPolicy));
    String committedFirst = SEEN_INSTANTS.get(0);

    HoodieDeltaStreamer.Config crashing = config(tableBasePath, transformers(CrashingAfterWriteTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    crashing.sourceLimit = 2000;
    Throwable crash = assertThrows(Throwable.class, () -> syncOnce(withCleaningPolicy(crashing, cleaningPolicy)));
    assertTrue(causes(crash).contains("simulated crash"), causes(crash));
    String crashed = SEEN_INSTANTS.get(1);
    List<HoodieInstant> pending = metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().getInstants();
    assertEquals(1, pending.size(), pending.toString());
    assertEquals(crashed, pending.get(0).requestedTime());
    assertEquals(HoodieInstant.State.INFLIGHT, pending.get(0).getState(), "the crash left the instant INFLIGHT");
    assertTrue(fs.exists(payloadPath(tableBasePath, crashed)), "the crash left its marked file");
    assertTrue(fs.exists(new Path(metaClient(tableBasePath).getMarkerFolderPath(crashed))), "and its markers");

    HoodieDeltaStreamer.Config restart = config(tableBasePath, transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    restart.sourceLimit = 2000;
    syncOnce(withCleaningPolicy(restart, cleaningPolicy));
    String committedAfter = SEEN_INSTANTS.get(2);

    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(Arrays.asList(committedFirst, committedAfter), completedCommits(tableBasePath));
    assertEquals(0, metaClient.getActiveTimeline().filterInflightsAndRequested().countInstants(), "nothing left pending");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), rollbacks.toString());
    assertEquals(crashed, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertFalse(fs.exists(payloadPath(tableBasePath, crashed)), "the standard rollback removed the crashed instant's marked file");
    assertFalse(fs.exists(new Path(metaClient.getMarkerFolderPath(crashed))), "and its markers");
    assertTrue(fs.exists(payloadPath(tableBasePath, committedFirst)), "committed payloads are kept");
    assertTrue(fs.exists(payloadPath(tableBasePath, committedAfter)), "committed payloads are kept");
  }

  private static HoodieDeltaStreamer.Config withCleaningPolicy(HoodieDeltaStreamer.Config cfg, String cleaningPolicy) {
    cfg.configs.removeIf(value -> value.startsWith(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "="));
    cfg.configs.add(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "=" + cleaningPolicy);
    if ("LAZY".equals(cleaningPolicy)) {
      cfg.configs.add("hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL");
      cfg.configs.add("hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider");
    }
    return cfg;
  }

  /**
   * A source timeout after the transformer returned, in the same attempt, is retried: the retry releases the instant the first
   * attempt started and held (its heartbeat stops), starts a new one, and the round commits that one; the round's LAZY cleaning
   * then rolls the released instant back, removing its marked files.
   */
  @Test
  void instantAwareSourceTimeoutRetryReleasesTheHeldInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_timeout_retry";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(TimeoutArmingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    cfg.schemaProviderClassName = TimeoutOnceSchemaProvider.class.getName();
    cfg.retryOnSourceFailures = true;
    cfg.maxRetryCount = 2;
    cfg.retryIntervalSecs = 0;
    TIMEOUT_ARMED.set(false);
    syncOnce(cfg);

    assertEquals(2, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    String held = SEEN_INSTANTS.get(0);
    String committed = SEEN_INSTANTS.get(1);
    assertEquals(Collections.singletonList(committed), completedCommits(tableBasePath));
    HoodieTableMetaClient metaClient = metaClient(tableBasePath);
    assertEquals(0, metaClient.getActiveTimeline().filterInflightsAndRequested().countInstants(), "nothing left pending");
    List<HoodieInstant> rollbacks = metaClient.getActiveTimeline().getRollbackTimeline().filterCompletedInstants().getInstants();
    assertEquals(1, rollbacks.size(), "the released instant, its heartbeat gone, was rolled back by LAZY cleaning");
    assertEquals(held, metaClient.getActiveTimeline().readRollbackMetadata(rollbacks.get(0)).getCommitsRollback().get(0));
    assertFalse(fs.exists(payloadPath(tableBasePath, held)), "the rolled-back instant's marked file is removed");
    assertTrue(fs.exists(payloadPath(tableBasePath, committed)), "the committed instant keeps its payload");
  }

  /**
   * With a 1 s heartbeat interval (the minimum) and two tolerable misses, a write that runs 5 s after the write client was
   * re-created commits only if the new client keeps the heartbeat of the pending instant.
   */
  @Test
  void instantAwareHeartbeatSurvivesWriteClientReinitialisation() throws Exception {
    String tableBasePath = basePath + "/early_commit_heartbeat";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, transformers(BlobWritingTransformer.class, SlowRowsTransformer.class));
    cfg.configs.add(HoodieWriteConfig.CLIENT_HEARTBEAT_INTERVAL_IN_MS.key() + "=1000");
    cfg.configs.add(HoodieWriteConfig.CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.key() + "=2");
    syncOnce(cfg);

    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath), "the round committed the instant started before the transformer");
    assertNull(metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().firstInstant().orElse(null));
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }
}
