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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieRestoreException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Objects;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for COPY_ON_WRITE table savepoint restore.
 */
@Tag("functional")
public class TestSavepointRestoreCopyOnWrite extends HoodieClientTestBase {

  /**
   * Actions: C1, C2, savepoint C2, C3, C4, restore.
   * Should go back to C2,
   * C3 and C4 should be cleaned up.
   */
  @Test
  void testBasicRollback() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 4; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(40);
      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }

  /**
   * Actions: C1, C2, savepoint C2, C3 whose commit metadata sets {@code hoodie.restore.unsupported.reason}, restore.
   * The restore is refused before anything is planned or rolled back: C3 stays completed and no restore instant is written.
   */
  @Test
  void testRestoreRefusedOverCommitMarkedRestoreUnsupported() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
      }
      savepointCommit = prevInstant;
      client.savepoint("user1", "Savepoint for 2nd commit");

      String markedCommit = client.startCommit();
      JavaRDD<WriteStatus> statuses = client.insert(jsc.parallelize(dataGen.generateInserts(markedCommit, numRecords), 1), markedCommit);
      client.commit(markedCommit, statuses, Option.of(Collections.singletonMap(
          RestorePlanActionExecutor.RESTORE_UNSUPPORTED_REASON_KEY, "files outside the write stats")));
      assertRowNumberEqualsTo(30);

      String restoreTo = savepointCommit;
      HoodieRestoreException failed = assertThrows(HoodieRestoreException.class, () -> client.restoreToSavepoint(restoreTo));
      Throwable refusal = failed;
      while (refusal != null && !(refusal instanceof HoodieRestoreException
          && String.valueOf(refusal.getMessage()).contains(RestorePlanActionExecutor.RESTORE_UNSUPPORTED_REASON_KEY))) {
        refusal = refusal.getCause();
      }
      assertTrue(refusal != null, "the refusal names the key: " + failed);
      assertTrue(refusal.getMessage().contains("files outside the write stats"), refusal.getMessage());
      assertTrue(refusal.getMessage().contains(markedCommit), refusal.getMessage());

      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().containsInstant(markedCommit));
      assertTrue(metaClient.getActiveTimeline().getRestoreTimeline().empty(), "no restore instant is written");
      assertRowNumberEqualsTo(30);
    }
  }

  /**
   * Actions: C1, C2, savepoint C2, C3 marked {@code hoodie.restore.unsupported.reason}, C4, C5, with the metadata table compacted
   * after C2 (as in testRollbackBeyondLastMDTCompaction), then restore to C2 (which would delete the metadata table first) and the procedure's pre-restore call.
   * Both are refused before the metadata table is touched.
   */
  @Test
  void testRefusedRestoreKeepsTheMetadataTable() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
      }
      String savepointCommit = prevInstant;
      client.savepoint("user1", "Savepoint for 2nd commit");

      String markedCommit = client.startCommit();
      JavaRDD<WriteStatus> statuses = client.insert(jsc.parallelize(dataGen.generateInserts(markedCommit, numRecords), 1), markedCommit);
      client.commit(markedCommit, statuses, Option.of(Collections.singletonMap(
          RestorePlanActionExecutor.RESTORE_UNSUPPORTED_REASON_KEY, "files outside the write stats")));
      prevInstant = markedCommit;
      for (int i = 4; i <= 5; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
      }

      String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(metadataTableBasePath).setConf(storageConf).setLoadActiveTimelineOnLoad(true).build();
      Option<HoodieInstant> oldestMdtCompaction = metadataMetaClient.getCommitTimeline().filterCompletedInstants().firstInstant();
      assertTrue(oldestMdtCompaction.isPresent() && oldestMdtCompaction.get().requestedTime().compareTo(savepointCommit) >= 0,
          "precondition: the savepoint is at or before the oldest metadata table compaction, so a restore deletes the MDT first");
      int mdtCommits = metadataMetaClient.getActiveTimeline().filterCompletedInstants().countInstants();
      StoragePath mdtPath = new StoragePath(metadataTableBasePath);

      assertThrows(HoodieRestoreException.class, () -> client.restoreToSavepoint(savepointCommit));
      assertMetadataTableKept(mdtPath, metadataMetaClient, mdtCommits, "a refused restoreToSavepoint");
      // the pre-restore call restore_to_instant makes
      assertThrows(HoodieRestoreException.class, () -> client.deleteMdtIfNecessaryBeforeRestore(savepointCommit));
      assertMetadataTableKept(mdtPath, metadataMetaClient, mdtCommits, "a refused deleteMdtIfNecessaryBeforeRestore");

      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertTrue(metaClient.getActiveTimeline().getRestoreTimeline().empty(), "no restore instant is written");
      assertRowNumberEqualsTo(50);
    }
  }

  /** A restore to an instant that is not a savepoint, and is not on the timeline at all, reports the missing savepoint. */
  @Test
  void testRestoreToMissingSavepointReportsIt() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String commit = WriteClientTestUtils.createNewInstantTime();
      insertBatch(hoodieWriteConfig, client, commit, HoodieTimeline.INIT_INSTANT_TS, 10, SparkRDDWriteClient::insert,
          false, true, 10, 10, 1, Option.empty(), INSTANT_GENERATOR);
      HoodieRollbackException missing = assertThrows(HoodieRollbackException.class, () -> client.restoreToSavepoint("20000101000000000"));
      assertTrue(missing.getMessage().contains("No savepoint for instantTime 20000101000000000"), missing.getMessage());
    }
  }

  private void assertMetadataTableKept(StoragePath mdtPath, HoodieTableMetaClient metadataMetaClient, int mdtCommits, String after)
      throws Exception {
    assertTrue(HoodieTableMetaClient.reload(metaClient).getTableConfig().isMetadataTableAvailable(),
        after + " keeps the metadata partitions enabled");
    assertTrue(storage.exists(new StoragePath(mdtPath, HoodieTableMetaClient.METAFOLDER_NAME + "/hoodie.properties")),
        after + " keeps the metadata table");
    assertEquals(mdtCommits, HoodieTableMetaClient.reload(metadataMetaClient).getActiveTimeline().filterCompletedInstants().countInstants(),
        after + " keeps the metadata table's commits");
  }

  @Test
  void testRollbackBeyondLastMDTCompaction() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      final int iterations = 5;
      for (int i = 1; i <= 5; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(iterations * numRecords);
      // restore will be forced to rebuild the metadata table
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
      // check if the metadata table is rebuilt
      String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(metadataTableBasePath)
          .setConf(storageConf)
          .setLoadActiveTimelineOnLoad(true)
          .build();
      assertEquals(1, metadataMetaClient.getCommitsTimeline().filter(instant -> !instant.requestedTime().startsWith(SOLO_COMMIT_TIMESTAMP)).countInstants());
    }
  }

  /**
   * The restore should roll back all the pending instants that are beyond the savepoint.
   *
   * <p>Actions: C1, C2, savepoint C2, C3, C4 inflight, restore.
   * Should go back to C2,
   * C3, C4 should be cleaned up.
   */
  @Test
  void testCleaningPendingInstants() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 3; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(30);
      // write another pending instant
      insertBatchWithoutCommit(numRecords);
      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }

  /**
   * The rollbacks(either inflight or complete) beyond the savepoint should be cleaned.
   *
   * <p>Actions: C1, C2, savepoint C2, C3, C4 (RB_C3), C5, restore.
   * Should go back to C2.
   * C3, C4(RB_C3), C5 should be cleaned up.
   *
   * <p>Actions: C1, C2, savepoint C2, C3, C4 (RB_C3) inflight, restore.
   * Should go back to C2.
   * C3, C4 (RB_C3) should be cleaned up.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCleaningRollbackInstants(boolean commitRollback) throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        // eager cleaning
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 2; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        // Write 4 inserts with the 2nd commit been rolled back
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          // trigger savepoint
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(20);
      // write another pending instant
      insertBatchWithoutCommit(numRecords);
      // rollback the pending instant
      if (commitRollback) {
        client.rollbackFailedWrites(metaClient);
      } else {
        HoodieInstant pendingInstant = metaClient.getActiveTimeline().filterPendingExcludingCompaction()
            .lastInstant().orElseThrow(() -> new HoodieException("Pending instant does not exist"));
        HoodieSparkTable.create(client.getConfig(), context)
            .scheduleRollback(context, WriteClientTestUtils.createNewInstantTime(), pendingInstant, false, true, false);
      }
      Option<String> rollbackInstant = metaClient.reloadActiveTimeline().getRollbackTimeline().lastInstant().map(HoodieInstant::requestedTime);
      assertTrue(rollbackInstant.isPresent(), "The latest instant should be a rollback");
      // write another batch
      insertBatch(hoodieWriteConfig, client, WriteClientTestUtils.createNewInstantTime(),
          rollbackInstant.get(), numRecords, SparkRDDWriteClient::insert,
          false, true, numRecords, numRecords * 3, 1, Option.empty(), INSTANT_GENERATOR);
      // restore
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }

  /**
   * Two-assertion test for the MDT pre-check guard:
   * <ol>
   *   <li>{@code deleteMdtIfNecessaryBeforeRestore(firstCommit)} returns {@code true} (and
   *       deletes the MDT) when the target is at or before the oldest MDT compaction,
   *       confirming the guard logic fires for this table setup.</li>
   *   <li>{@code restoreToInstant(firstCommit, false)} does NOT invoke the pre-check, so a
   *       caller that explicitly opts out of MDT integration is never surprised by an implicit
   *       MDT deletion.</li>
   * </ol>
   */
  @Test
  void testRestoreToInstantSkipsMdtCheckWhenMetadataDisabled() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(true)
            .withMaxNumDeltaCommitsBeforeCompaction(4)
            .build())
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String firstCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      // 5 inserts so the MDT goes through one compaction with maxDeltaCommits=4.
      for (int i = 1; i <= 5; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 1) {
          firstCommit = newCommitTime;
        }
      }
      assertRowNumberEqualsTo(50);

      String mdtBasePath = HoodieTableMetadata.getMetadataTableBasePath(hoodieWriteConfig.getBasePath());
      assertTrue(HoodieStorageUtils.getStorage(mdtBasePath, storageConf).exists(new StoragePath(mdtBasePath)),
          "MDT directory should exist before any pre-check");

      // Assertion 1: deleteMdtIfNecessaryBeforeRestore detects that firstCommit is at or before
      // the oldest MDT compaction, deletes the MDT, and returns true.
      boolean mdtDeleted = client.deleteMdtIfNecessaryBeforeRestore(
          Objects.requireNonNull(firstCommit, "first commit should not be null"));
      assertTrue(mdtDeleted,
          "deleteMdtIfNecessaryBeforeRestore should return true when target is at or before the oldest MDT compaction");
      assertFalse(HoodieStorageUtils.getStorage(mdtBasePath, storageConf).exists(new StoragePath(mdtBasePath)),
          "MDT directory should have been deleted by deleteMdtIfNecessaryBeforeRestore");

      // Assertion 2: restoreToInstant with initialMetadataTableIfNecessary=false does NOT invoke
      // the pre-check — the MDT (now absent) is not touched. The restore proceeds without MDT.
      client.restoreToInstant(firstCommit, false);
      assertRowNumberEqualsTo(numRecords);
    }
  }

  /**
   * Regression coverage for the {@code restoreToSavepoint} refactor. After replacing the inline
   * MDT pre-check with a call to the centralized helper, the common case (target after the
   * oldest MDT compaction, no MDT delete needed) must still work end-to-end.
   */
  @Test
  void testRestoreToSavepointStillWorksAfterRefactor() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withRollbackUsingMarkers(true)
        .build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig)) {
      String savepointCommit = null;
      String prevInstant = HoodieTimeline.INIT_INSTANT_TS;
      final int numRecords = 10;
      for (int i = 1; i <= 4; i++) {
        String newCommitTime = WriteClientTestUtils.createNewInstantTime();
        insertBatch(hoodieWriteConfig, client, newCommitTime, prevInstant, numRecords, SparkRDDWriteClient::insert,
            false, true, numRecords, numRecords * i, 1, Option.empty(), INSTANT_GENERATOR);
        prevInstant = newCommitTime;
        if (i == 2) {
          savepointCommit = newCommitTime;
          client.savepoint("user1", "Savepoint for 2nd commit");
        }
      }
      assertRowNumberEqualsTo(40);
      client.restoreToSavepoint(Objects.requireNonNull(savepointCommit, "restore commit should not be null"));
      assertRowNumberEqualsTo(20);
    }
  }
}
