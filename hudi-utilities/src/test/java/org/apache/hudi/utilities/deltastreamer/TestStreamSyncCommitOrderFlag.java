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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * How StreamSync orders the commit and the transformer, selected by the transformer chain alone: a chain with no
 * {@link InstantAwareTransformer} runs as stock (every transformer through the plain apply, before any instant exists); a chain
 * with one makes StreamSync start the commit (REQUESTED) once the source returned data, move it to INFLIGHT right before the first
 * {@link InstantAwareTransformer}, and pass that instant, which is the instant the round then commits. There is no option.
 */
@Tag("functional")
public class TestStreamSyncCommitOrderFlag extends HoodieDeltaStreamerTestBase {

  /** Property naming the target base path, so the transformer can read the table's timeline. */
  private static final String TARGET_BASE_PATH = "test.commit.order.flag.target.base.path";
  /** One entry per call: the instant passed, or an empty string for a call through the plain apply. */
  private static final List<String> SEEN_INSTANTS = new CopyOnWriteArrayList<>();
  private static final List<Boolean> MULTI_WRITE_AT_COMMIT = new CopyOnWriteArrayList<>();
  /** One entry per call: the requested or inflight instants of the target table at the time of the call. */
  private static final List<List<HoodieInstant>> PENDING_AT_APPLY = new CopyOnWriteArrayList<>();

  @BeforeEach
  void clearCalls() {
    SEEN_INSTANTS.clear();
    MULTI_WRITE_AT_COMMIT.clear();
    PENDING_AT_APPLY.clear();
  }

  /** A plain transformer that records each call and the pending instants at that moment; returns its input unchanged. */
  public static class PlainRecordingTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      record(properties, "");
      return rowDataset;
    }

    @Override
    public void onPreCommit(JavaSparkContext jsc, SparkSession spark, TypedProperties properties, String instantTime,
                            HoodieTableMetaClient table) {
      MULTI_WRITE_AT_COMMIT.add(properties.getBoolean(HoodieWriteConfig.ALLOW_MULTI_WRITE_ON_SAME_INSTANT_ENABLE.key(), false));
    }
  }

  /** An {@link InstantAwareTransformer} that records each call and the pending instants at that moment; returns its input. */
  public static class RecordingTransformer extends PlainRecordingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      record(properties, instantTime);
      return rowDataset;
    }
  }

  private static void record(TypedProperties properties, String instantTime) {
    SEEN_INSTANTS.add(instantTime);
    String basePath = properties.getString(TARGET_BASE_PATH);
    try {
      PENDING_AT_APPLY.add(fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
          ? HoodieTestUtils.createMetaClient(storage, basePath).getActiveTimeline().filterInflightsAndRequested().getInstants()
          : Collections.emptyList());
    } catch (IOException e) {
      throw new HoodieTransformException("Cannot read the timeline of " + basePath, e);
    }
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, Class<?> recordingTransformer) {
    List<String> transformers = Arrays.asList(recordingTransformer.getName(),
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName());
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, transformers);
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    return cfg;
  }

  private static List<String> completedCommits(String tableBasePath) {
    return HoodieTestUtils.createMetaClient(storage, tableBasePath).getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  @Test
  void productionConstructorAcceptsAJobWithoutTransformers() throws Exception {
    String tableBasePath = basePath + "/commit_order_no_transformers";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, Collections.emptyList());
    HoodieStreamer streamer = new HoodieDeltaStreamer(cfg, jsc);
    try {
      assertEquals(Collections.emptyList(), SEEN_INSTANTS);
      assertEquals(Collections.emptyList(), MULTI_WRITE_AT_COMMIT);
    } finally {
      streamer.shutdownGracefully();
    }
  }

  @Test
  void chainWithoutInstantAwareTransformerCallsThePlainApplyBeforeAnyInstant() throws Exception {
    String tableBasePath = basePath + "/commit_order_plain_chain";
    syncOnce(config(tableBasePath, PlainRecordingTransformer.class));

    assertEquals(Collections.singletonList(false), MULTI_WRITE_AT_COMMIT, "plain writers retain default same-instant rejection");
    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "called through the plain apply, without an instant");
    assertEquals(Collections.singletonList(Collections.emptyList()), PENDING_AT_APPLY, "no instant was pending at apply");
    assertEquals(1, completedCommits(tableBasePath).size());
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  @Test
  void chainWithInstantAwareTransformerPassesTheInflightInstantThatTheRoundCommits() throws Exception {
    String tableBasePath = basePath + "/commit_order_instant_aware_chain";
    syncOnce(config(tableBasePath, RecordingTransformer.class));

    assertEquals(Collections.singletonList(true), MULTI_WRITE_AT_COMMIT, "only the instant-aware handoff enables same-instant writes");
    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    String instant = SEEN_INSTANTS.get(0);
    assertNotEquals("", instant, "called with an instant, with no option set");
    List<HoodieInstant> pending = PENDING_AT_APPLY.get(0);
    assertEquals(1, pending.size(), pending.toString());
    assertEquals(instant, pending.get(0).requestedTime(), "the pending instant is the one passed to the transformer");
    assertEquals(HoodieInstant.State.INFLIGHT, pending.get(0).getState(), "the instant is INFLIGHT at apply");
    assertEquals(Collections.singletonList(instant), completedCommits(tableBasePath), "the round committed that instant");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }
}
