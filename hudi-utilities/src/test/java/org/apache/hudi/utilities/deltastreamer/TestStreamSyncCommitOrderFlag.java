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
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.exception.HoodieTransformException;
import org.apache.hudi.utilities.transform.InstantAwareTransformer;

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

/**
 * {@code hoodie.streamer.start.commit.before.transformer} in StreamSync: off (the default), an {@link InstantAwareTransformer}
 * is called through the plain apply before any instant exists; on, StreamSync starts the commit first and
 * passes the INFLIGHT instant, which is the instant the round then commits.
 */
@Tag("functional")
public class TestStreamSyncCommitOrderFlag extends HoodieDeltaStreamerTestBase {

  /** Property naming the target base path, so the transformer can read the table's timeline. */
  private static final String TARGET_BASE_PATH = "test.commit.order.flag.target.base.path";
  /** One entry per call: the instant passed, or an empty string for a call through the plain apply. */
  private static final List<String> SEEN_INSTANTS = new CopyOnWriteArrayList<>();
  /** One entry per call: the requested or inflight instants of the target table at the time of the call. */
  private static final List<List<HoodieInstant>> PENDING_AT_APPLY = new CopyOnWriteArrayList<>();

  @BeforeEach
  void clearCalls() {
    SEEN_INSTANTS.clear();
    PENDING_AT_APPLY.clear();
  }

  /** Records each call and the pending instants at that moment; returns its input unchanged. */
  public static class RecordingTransformer implements InstantAwareTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      record(properties, "");
      return rowDataset;
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                              String instantTime) {
      record(properties, instantTime);
      return rowDataset;
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
  }

  private static HoodieDeltaStreamer.Config config(String tableBasePath, boolean flag) {
    List<String> transformers = Arrays.asList(RecordingTransformer.class.getName(),
        TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName());
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, transformers);
    if (flag) {
      cfg.configs.add(HoodieStreamerConfig.START_COMMIT_BEFORE_TRANSFORMER.key() + "=true");
    }
    cfg.configs.add(TARGET_BASE_PATH + "=" + tableBasePath);
    return cfg;
  }

  private static List<String> completedCommits(String tableBasePath) {
    return HoodieTestUtils.createMetaClient(storage, tableBasePath).getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }

  @Test
  void flagOffCallsThePlainApplyBeforeAnyInstant() throws Exception {
    String tableBasePath = basePath + "/commit_order_flag_off";
    syncOnce(config(tableBasePath, false));

    assertEquals(Collections.singletonList(""), SEEN_INSTANTS, "called through the plain apply, without an instant");
    assertEquals(Collections.singletonList(Collections.emptyList()), PENDING_AT_APPLY, "no instant was pending at apply");
    assertEquals(1, completedCommits(tableBasePath).size());
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }

  @Test
  void flagOnPassesTheInflightInstantThatTheRoundCommits() throws Exception {
    String tableBasePath = basePath + "/commit_order_flag_on";
    syncOnce(config(tableBasePath, true));

    assertEquals(1, SEEN_INSTANTS.size(), SEEN_INSTANTS.toString());
    String instant = SEEN_INSTANTS.get(0);
    List<HoodieInstant> pending = PENDING_AT_APPLY.get(0);
    assertEquals(1, pending.size(), pending.toString());
    assertEquals(instant, pending.get(0).requestedTime(), "the pending instant is the one passed to the transformer");
    assertEquals(HoodieInstant.State.INFLIGHT, pending.get(0).getState(), "the instant is INFLIGHT at apply");
    assertEquals(Collections.singletonList(instant), completedCommits(tableBasePath), "the round committed that instant");
    assertEquals(SQL_SOURCE_NUM_RECORDS, sqlContext.read().format("hudi").load(tableBasePath).count());
  }
}
