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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;

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
 * {@code hoodie.streamer.start.commit.before.transformer}: with the flag on, StreamSync starts the commit before the transformer,
 * an {@link InstantAwareTransformer} receives the INFLIGHT instant and can write marked files under {@code .hoodie/.blob/}, a
 * transformer failure rolls that instant and its marked files back, and the heartbeat of the instant survives the
 * re-initialisation of the write client. With the flag off (the default) the transformer runs before any instant, as before.
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
    assertFalse(fs.exists(new Path(tableBasePath, HoodieTableMetaClient.BLOB_FOLDER_NAME)), "nothing written under .hoodie/.blob");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  /**
   * Two rounds (an empty table, then one with a schema): each round's transformer receives the INFLIGHT instant that the
   * round then commits and writes under its folder; the plain transformer of the chain still runs.
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
      assertTrue(fs.exists(payloadPath(tableBasePath, instant)), "the payload of " + instant + " is in its folder");
    }
    Dataset<Row> table = sqlContext.read().format("hudi").load(tableBasePath);
    long rows = table.count();
    assertTrue(rows > SQL_SOURCE_NUM_RECORDS, "both rounds wrote rows: " + rows);
    assertEquals(rows, table.where("haversine_distance is not null").count(), "the plain transformer of the chain ran");
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
   * A transformer failure rolls back the instant while it is still INFLIGHT, with eager or lazy OCC cleaning.
   * The marker directory and heartbeat are removed after rollback.
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
    assertTrue(fs.exists(partitionedPayloadPath(tableBasePath, instant).getParent().getParent()),
        "the partition ancestors remain");
    assertFalse(fs.exists(new Path(metaClient.getMarkerFolderPath(instant))), "rollback removed its marker directory");
    assertFalse(fs.exists(new Path(HoodieTableMetaClient.getHeartbeatFolderPath(tableBasePath), instant)), "heartbeat stopped");
  }

  /**
   * A source timeout after the transformer returned, in the same attempt, is retried: the retry releases the instant the first
   * attempt started and held (its heartbeat stops), starts a new one, and the round commits that one; the round's LAZY cleaning
   * then rolls the released instant back.
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
