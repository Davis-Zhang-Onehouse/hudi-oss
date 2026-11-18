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

package org.apache.hudi.client.transaction;

import org.apache.hudi.common.heartbeat.HoodieHeartbeatUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * This class extends the base implementation of conflict resolution strategy.
 * It gives preference to non-blocking ingestion over table services in case of conflicts.
 */
@Slf4j
public class PreferWriterConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  private boolean isClusteringBlockForPendingIngestion;

  /**
   * For tableservices like replacecommit and compaction commits this method also returns ingestion inflight commits.
   */
  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<HoodieInstant> lastSuccessfulInstant) {
    return getCandidateInstants(metaClient, currentInstant, lastSuccessfulInstant, Option.empty());
  }

  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<HoodieInstant> lastSuccessfulInstant, Option<HoodieWriteConfig> writeConfigOpt) {
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    boolean isCurrentOperationClustering = ClusteringUtils.isClusteringInstant(activeTimeline, currentInstant, metaClient.getInstantGenerator());
    this.isClusteringBlockForPendingIngestion = isCurrentOperationClustering
        && writeConfigOpt.isPresent() && writeConfigOpt.get().isClusteringBlockForPendingIngestion();

    if (isCurrentOperationClustering || COMPACTION_ACTION.equals(currentInstant.getAction())) {
      // Table service rollbacks are done by table service jobs/writers only, not by ingestion threads,
      // so rollback conflict detection is not needed for table services.
      return getCandidateInstantsForTableServicesCommits(activeTimeline, currentInstant, isCurrentOperationClustering, metaClient, writeConfigOpt);
    } else {
      return Stream.concat(getCandidateInstantsForNonTableServicesCommits(activeTimeline, currentInstant),
          getCandidateInstantsForRollbackConflict(activeTimeline, currentInstant));
    }
  }

  private Stream<HoodieInstant> getCandidateInstantsForRollbackConflict(HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant) {
    // Add Requested rollback action instants that were created after the current instant.
    List<HoodieInstant> pendingRollbacks = activeTimeline
        .findInstantsAfter(currentInstant.requestedTime())
        .filterPendingRollbackTimeline()
        .getInstantsAsStream().collect(Collectors.toList());
    log.info(String.format("Rollback instants that may have conflict with %s are %s", currentInstant, pendingRollbacks));
    return pendingRollbacks.stream();
  }

  private Stream<HoodieInstant> getCandidateInstantsForNonTableServicesCommits(HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant) {

    // To find out which instants are conflicting, we apply the following logic
    // Get all the completed instants timeline only for commits that have happened
    // since the last successful write based on the transition times.
    // We need to check for write conflicts since they may have mutated the same files
    // that are being newly created by the current write.
    List<HoodieInstant> completedCommitsInstants = activeTimeline
        .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
        .filterCompletedInstants()
        .findInstantsModifiedAfterByCompletionTime(currentInstant.requestedTime())
        .getInstantsOrderedByCompletionTime()
        .collect(Collectors.toList());
    log.info("Instants that may have conflict with {} are {}", currentInstant, completedCommitsInstants);
    return completedCommitsInstants.stream();
  }

  /**
   * Returns candidate instants for table service commits (clustering or compaction).
   * Includes both completed instants and ingestion inflight commits that have happened
   * since the current write started.
   *
   * <p>If the current write is clustering and
   * {@code hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution} is enabled,
   * also includes ingestion {@code .requested} instants, and ingestion {@code .inflight} instants whose inflight file is still
   * empty (filtering out both with expired heartbeats), so they can be evaluated by {@link #hasConflict} and
   * {@link #resolveConflict}. An empty inflight file is written by any writer that moves its instant to inflight before it writes
   * a workload profile (the streamer before an instant-aware transformer, and bulk insert, the row writer and Flink, which keep
   * it empty for the whole write); with EAGER failed-writes cleaning there is no heartbeat, so such an instant, like a
   * .requested one, never blocks clustering.</p>
   */
  private Stream<HoodieInstant> getCandidateInstantsForTableServicesCommits(
      HoodieActiveTimeline activeTimeline, HoodieInstant currentInstant,
      boolean isCurrentOperationClustering, HoodieTableMetaClient metaClient,
      Option<HoodieWriteConfig> writeConfigOpt) {

    Stream<HoodieInstant> completedCommitsStream =
        activeTimeline
            .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, DELTA_COMMIT_ACTION))
            .filterCompletedInstants()
            .findInstantsModifiedAfterByCompletionTime(currentInstant.requestedTime())
            .getInstantsAsStream();

    Stream<HoodieInstant> inflightIngestionCommitsStream;
    if (isClusteringBlockForPendingIngestion) {
      HoodieWriteConfig writeConfig = writeConfigOpt.get();
      long maxHeartbeatIntervalMs = writeConfig.getHoodieClientHeartbeatIntervalInMs()
          * (writeConfig.getHoodieClientHeartbeatTolerableMisses() + 1);
      inflightIngestionCommitsStream = activeTimeline
          .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION))
          .filterInflightsAndRequested()
          .getInstantsAsStream()
          .filter(i -> !ClusteringUtils.isClusteringInstant(activeTimeline, i, metaClient.getInstantGenerator()))
          .filter(i -> {
            if (i.isRequested()) {
              return isHeartbeatActive(metaClient, i, maxHeartbeatIntervalMs);
            }
            if (!i.isInflight()) {
              return false;
            }
            Option<Long> inflightFileLength = inflightFileLength(metaClient, i);
            if (!inflightFileLength.isPresent()) {
              // rolled back after the timeline was read: no candidate, as a completed rollback leaves none
              return false;
            }
            // an INFLIGHT instant with an empty inflight file has not written its workload profile yet: like a .requested
            // one it names no file group, and it is a candidate only while its writer's heartbeat is active
            return inflightFileLength.get() > 0 || isHeartbeatActive(metaClient, i, maxHeartbeatIntervalMs);
          });
    } else {
      inflightIngestionCommitsStream = activeTimeline
          .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION))
          .filterInflights()
          .getInstantsAsStream();
    }

    List<HoodieInstant> instantsToConsider = Stream.concat(completedCommitsStream, inflightIngestionCommitsStream)
        .sorted(Comparator.comparing(HoodieInstant::getCompletionTime, Comparator.nullsLast(Comparator.naturalOrder())))
        .collect(Collectors.toList());
    log.info("Instants that may have conflict with {} are {}", currentInstant, instantsToConsider);
    return instantsToConsider.stream();
  }

  private static boolean isHeartbeatActive(HoodieTableMetaClient metaClient, HoodieInstant instant, long maxHeartbeatIntervalMs) {
    try {
      return !HoodieHeartbeatUtils.isHeartbeatExpired(instant.requestedTime(), maxHeartbeatIntervalMs,
          metaClient.getStorage(), metaClient.getBasePath().toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * The length of the timeline file of {@code instant}; empty when the file is gone (the instant was rolled back since). Existence is
   * asked first: with the consistency check on, a stat of a file that is not there waits the consistency guard's full backoff.
   */
  static Option<Long> inflightFileLength(HoodieTableMetaClient metaClient, HoodieInstant instant) {
    StoragePath path = new StoragePath(metaClient.getTimelinePath(), metaClient.getInstantFileNameGenerator().getFileName(instant));
    try {
      if (!metaClient.getStorage().exists(path)) {
        return Option.empty();
      }
      return Option.of(metaClient.getStorage().getPathInfo(path).getLength());
    } catch (FileNotFoundException e) {
      return Option.empty();
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read the inflight file " + path, e);
    }
  }

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    if (isClusteringBlockForPendingIngestion
        && WriteOperationType.CLUSTER.equals(thisOperation.getOperationType())
        && isPendingIngestionWithoutWorkloadProfile(otherOperation)) {
      log.info("Clustering operation {} conflicts with pending ingestion instant {} "
          + "that has an active heartbeat", thisOperation, otherOperation);
      return true;
    }
    return super.hasConflict(thisOperation, otherOperation);
  }

  @Override
  public Option<HoodieCommitMetadata> resolveConflict(HoodieTable table,
      ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    if (isClusteringBlockForPendingIngestion
        && WriteOperationType.CLUSTER.equals(thisOperation.getOperationType())
        && isPendingIngestionWithoutWorkloadProfile(otherOperation)) {
      throw new HoodieWriteConflictException(
          HoodieWriteConflictException.ConflictCategory.TABLE_SERVICE_VS_INGESTION,
          String.format("Pending ingestion instant %s with active heartbeat has not written its workload profile "
              + "yet but may potentially conflict with current clustering operation %s",
              otherOperation, thisOperation));
    }
    return super.resolveConflict(table, thisOperation, otherOperation);
  }

  /**
   * Whether {@code operation} is an ingestion instant that does not name the file groups it writes yet: a .requested one, or
   * one that is INFLIGHT with an empty inflight file (no operation type, no file group), as when a writer moves its instant to
   * inflight before the write writes its workload profile. Clustering cannot tell which file groups such an instant writes.
   */
  private boolean isPendingIngestionWithoutWorkloadProfile(ConcurrentOperation operation) {
    String state = operation.getInstantActionState();
    String actionType = operation.getInstantActionType();
    boolean ingestionAction = COMMIT_ACTION.equals(actionType) || DELTA_COMMIT_ACTION.equals(actionType)
        || (REPLACE_COMMIT_ACTION.equals(actionType) && !WriteOperationType.CLUSTER.equals(operation.getOperationType()));
    if (!ingestionAction) {
      return false;
    }
    if (HoodieInstant.State.REQUESTED.name().equals(state)) {
      return true;
    }
    return HoodieInstant.State.INFLIGHT.name().equals(state)
        && (operation.getOperationType() == null || WriteOperationType.UNKNOWN.equals(operation.getOperationType()))
        && operation.getMutatedPartitionAndFileIds().isEmpty();
  }

  @Override
  public boolean isPreCommitRequired() {
    return true;
  }
}
