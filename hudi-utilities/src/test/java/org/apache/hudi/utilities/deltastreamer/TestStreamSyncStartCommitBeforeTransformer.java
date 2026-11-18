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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code hoodie.streamer.start.commit.before.transformer}: with the flag on, StreamSync starts the commit and moves it to INFLIGHT
 * before the transformer, an {@link InstantAwareTransformer} receives the INFLIGHT instant and writes marked files under
 * {@code .hoodie/.blob/} (markers after inflight, the normal protocol), the write of the round takes the instant that is already
 * INFLIGHT, the commit keeps the marked files its rows reference, a transformer failure or a crash leaves an INFLIGHT instant that
 * the standard marker-based rollback removes with its files, and the heartbeat of the instant survives the re-initialisation of the
 * write client. With the flag off (the default) the transformer runs before any instant, as before.
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

  @BeforeEach
  void clearCalls() {
    SEEN_INSTANTS.clear();
    PAYLOADS.clear();
    PARTITIONED_PAYLOADS.clear();
    PENDING_AT_APPLY.clear();
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
        String relative = org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter.forTable(storage.getConf(), base, properties)
            .forInstant(storage, instantTime)
            .write("", out -> { });
        PAYLOADS.put(base + "#" + instantTime, new Path(base, relative));
      } catch (IOException e) {
        throw new HoodieTransformException("Cannot write the payload of " + instantTime, e);
      }
      return rowDataset;
    }

    private static void record(TypedProperties properties, String instantTime) {
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

  private static HoodieDeltaStreamer.Config config(String tableBasePath, boolean flag, List<String> transformers) {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, transformers);
    if (flag) {
      cfg.configs.add(HoodieStreamerConfig.START_COMMIT_BEFORE_TRANSFORMER.key() + "=true");
    }
    cfg.configs.add(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key() + "=LAZY");
    cfg.configs.add(HoodieWriteConfig.MARKERS_TYPE.key() + "=TIMELINE_SERVER_BASED");
    cfg.configs.add(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key() + "=true");
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    return cfg;
  }

  private static List<String> transformers(Class<?>... classes) {
    return Arrays.stream(classes).map(Class::getName).collect(Collectors.toList());
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

  /** The default order: the transformer runs through the plain apply before any instant exists, and nothing is written. */
  @Test
  void flagOffKeepsTheTransformerBeforeTheCommit() throws Exception {
    String tableBasePath = basePath + "/early_commit_flag_off";
    syncOnce(config(tableBasePath, false, transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class)));

    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "called through the plain apply, without an instant");
    assertEquals(Collections.singletonList(Collections.emptyList()), PENDING_AT_APPLY, "no instant was pending at apply");
    assertEquals(1, completedCommits(tableBasePath).size());
    assertFalse(fs.exists(new Path(tableBasePath, ManagedBlobPaths.FOLDER)), "nothing written under .hoodie/.blob");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * Two rounds (an empty table, then one with a schema): each round's transformer receives the instant, already INFLIGHT, that
   * the round then commits and writes a marked file under its folder; the commit's marker reconciliation keeps that file, which
   * no write stat names; the plain transformer of the chain still runs.
   */
  @Test
  void flagOnPassesTheRequestedInstantToTheTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_flag_on";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, true,
        transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
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
   * Restore refuses to roll back a commit whose instant was handed to the transformer before the write: the files the
   * transformer wrote under it are not in its write stats, so a restore would leave them behind. Commits written with the
   * option off are restored as before, and nothing changes when the restore is refused.
   */
  @Test
  void flagOnRestoreRefusesToRollBackARoundStartedBeforeTheTransformer() throws Exception {
    String tableBasePath = basePath + "/early_commit_restore";
    HoodieDeltaStreamer.Config on = config(tableBasePath, true,
        transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(on);
    String savepoint = completedCommits(tableBasePath).get(0);
    HoodieDeltaStreamer.Config off = config(tableBasePath, false, transformers(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    off.sourceLimit = 2000;
    syncOnce(off);
    assertEquals(2, completedCommits(tableBasePath).size());

    try (SparkRDDWriteClient<?> client = writeClient(tableBasePath)) {
      client.savepoint(savepoint, "test", "before the option-off round");
      client.restoreToSavepoint(savepoint);
    }
    assertEquals(Collections.singletonList(savepoint), completedCommits(tableBasePath), "the option-off round was restored away");

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
  void flagOnNoNewDataStartsNoInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_no_new_data";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, true,
        transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
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
  void flagOnTransformerFailureRollsBackTheRequestedInstant(String cleaningPolicy) throws Exception {
    String tableBasePath = basePath + "/early_commit_failure";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, true,
        transformers(FailingAfterWriteTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
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
  void flagOnCrashLeavesAnInflightInstantThatTheRestartRollsBack(String cleaningPolicy) throws Exception {
    String tableBasePath = basePath + "/early_commit_crash_" + cleaningPolicy.toLowerCase();
    // a first round, so the crashed instant is not the first instant of the table
    HoodieDeltaStreamer.Config first = config(tableBasePath, true,
        transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
    syncOnce(withCleaningPolicy(first, cleaningPolicy));
    String committedFirst = SEEN_INSTANTS.get(0);

    HoodieDeltaStreamer.Config crashing = config(tableBasePath, true,
        transformers(CrashingAfterWriteTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
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

    HoodieDeltaStreamer.Config restart = config(tableBasePath, true,
        transformers(BlobWritingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
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
  void flagOnSourceTimeoutRetryReleasesTheHeldInstant() throws Exception {
    String tableBasePath = basePath + "/early_commit_timeout_retry";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, true,
        transformers(TimeoutArmingTransformer.class, TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class));
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
  void flagOnHeartbeatSurvivesWriteClientReinitialisation() throws Exception {
    String tableBasePath = basePath + "/early_commit_heartbeat";
    HoodieDeltaStreamer.Config cfg = config(tableBasePath, true, transformers(BlobWritingTransformer.class, SlowRowsTransformer.class));
    cfg.configs.add(HoodieWriteConfig.CLIENT_HEARTBEAT_INTERVAL_IN_MS.key() + "=1000");
    cfg.configs.add(HoodieWriteConfig.CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES.key() + "=2");
    syncOnce(cfg);

    assertEquals(SEEN_INSTANTS, completedCommits(tableBasePath), "the round committed the instant started before the transformer");
    assertNull(metaClient(tableBasePath).getActiveTimeline().filterInflightsAndRequested().firstInstant().orElse(null));
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }
}
