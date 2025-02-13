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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link TableSchemaResolver}.
 */
public class TestCommitMetadataDeserilizer extends HoodieCommonTestHarness {

  public static final int REQUEST_TIME_LENGTH = 4;
  HoodieTestTable testTable;

  private static final String SCHEMA_WITHOUT_METADATA_STR = "{\n"
      + "  \"namespace\": \"example.avro\",\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"User\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"timestamp\", \"type\": \"long\"},\n"
      + "    {\"name\": \"_row_key\", \"type\": \"string\"},\n"
      + "    {\"name\": \"rider\", \"type\": \"string\"},\n"
      + "    {\"name\": \"driver\", \"type\": \"string\"}\n"
      + "  ]\n"
      + "}";

  private static Schema SCHEMA_WITHOUT_METADATA = new Schema.Parser().parse(SCHEMA_WITHOUT_METADATA_STR);
  private static Schema SCHEMA_WITH_METADATA = addMetadataFields(SCHEMA_WITHOUT_METADATA, false);

  /**
   * Pads a string number with leading zeros until it reaches the specified length.
   *
   * @param number The string number to pad
   * @param length The desired total length after padding
   * @return The padded string number
   * @throws IllegalArgumentException if the input number is longer than the desired length
   */
  public static String padWithLeadingZeros(String number, int length) {
    if (number == null) {
      throw new IllegalArgumentException("Input number cannot be null");
    }
    if (number.length() > length) {
      throw new IllegalArgumentException("Input number length " + number.length()
          + " is greater than desired length " + length);
    }
    return String.format("%0" + length + "d", Long.parseLong(number));
  }

  @BeforeEach
  public void setUp() throws Exception {
    if (basePath == null) {
      initPath();
    }
  }

  @AfterEach
  public void tearDown() {
    cleanMetaClient();
  }

  private static Stream<Arguments> testGetTableSchemaFromLatestCommitMetadataTestDimension() {
    return Stream.of(
      // enableMetadata, tableType, isCommit
      Arguments.of(true, HoodieTableType.COPY_ON_WRITE, "commitOrDeltaCommit"),
      Arguments.of(true, HoodieTableType.COPY_ON_WRITE, "replacementCommit"),
      Arguments.of(true, HoodieTableType.MERGE_ON_READ, "commitOrDeltaCommit"),
      Arguments.of(true, HoodieTableType.MERGE_ON_READ, "replacementCommit"),
      Arguments.of(false, HoodieTableType.COPY_ON_WRITE, "commitOrDeltaCommit"),
      Arguments.of(false, HoodieTableType.COPY_ON_WRITE, "replacementCommit"),
      Arguments.of(false, HoodieTableType.MERGE_ON_READ, "commitOrDeltaCommit"),
      Arguments.of(false, HoodieTableType.MERGE_ON_READ, "replacementCommit")
    );
  }

  // Covers all valid commit instants.
  @ParameterizedTest
  @MethodSource("testGetTableSchemaFromLatestCommitMetadataTestDimension")
  void testGetTableSchemaFromLatestCommitMetadata(boolean enableMetadata, HoodieTableType tableType, String type) throws Exception {
    initMetaClient(enableMetadata, tableType);
    testTable = HoodieTestTable.of(metaClient);

    String commitTime1 = "001";
    if (type.equals("commitOrDeltaCommit")) {
      // Case 1: Regular commit
      addCommitOrDeltaCommitWithSchema(tableType, commitTime1, SCHEMA_WITH_METADATA.toString());
    } else if (type.equals("replacementCommit")) {
      // Case 2: Replacement commit
      HoodieClusteringGroup group = new HoodieClusteringGroup();
      HoodieClusteringPlan plan = new HoodieClusteringPlan(
          Collections.singletonList(group),
          HoodieClusteringStrategy.newBuilder().build(),
          Collections.emptyMap(),
          1,
          false,
          null);
      HoodieRequestedReplaceMetadata requestedMetadata = new HoodieRequestedReplaceMetadata(
          WriteOperationType.UNKNOWN.name(),
          plan,
          Collections.emptyMap(),
          1);
      testTable.addReplaceCommit(commitTime1,
          Option.of(requestedMetadata),
          Option.empty(),
          (HoodieReplaceCommitMetadata)(buildMetadata(
              Collections.emptyList(),
              Collections.emptyMap(),
              Option.empty(),
              WriteOperationType.UNKNOWN,
              SCHEMA_WITH_METADATA.toString(),
              REPLACE_COMMIT_ACTION)));
    }

    metaClient.reloadActiveTimeline();
  }

  private void addCommitOrDeltaCommitWithSchema(HoodieTableType tableType, String commitTime1, String schemaStr) throws Exception {
    if (tableType == HoodieTableType.COPY_ON_WRITE) {
      testTable.addCommit(commitTime1, Option.of(buildMetadata(
          Collections.emptyList(),
          Collections.emptyMap(),
          Option.empty(),
          WriteOperationType.UNKNOWN,
          schemaStr,
          COMMIT_ACTION)));
    } else {
      testTable.addDeltaCommit(commitTime1, buildMetadata(
          Collections.emptyList(),
          Collections.emptyMap(),
          Option.empty(),
          WriteOperationType.UNKNOWN,
          schemaStr,
          DELTA_COMMIT_ACTION));
    }
  }

  private static Stream<Arguments> commonTableConfigTestDimension() {
    return Stream.of(
      // version 6 or 8, tableType
      Arguments.of(true, HoodieTableType.COPY_ON_WRITE),
      Arguments.of(true, HoodieTableType.MERGE_ON_READ),
      Arguments.of(false, HoodieTableType.COPY_ON_WRITE),
      Arguments.of(false, HoodieTableType.MERGE_ON_READ)
    );
  }

  private int createExhaustiveInstants(int startCommitTime, HoodieTableType tableType) throws Exception {
    if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
      testTable.addCompaction(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH),
          buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.COMPACT, SCHEMA_WITH_METADATA.toString(), COMPACTION_ACTION));
    }
    startCommitTime += 1;
    // Clean
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""),
        "", "", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
    HoodieCleanMetadata cleanMeta = new HoodieCleanMetadata("", 0L, 0, "20", "",
        Collections.emptyMap(), metaClient.getTableConfig().getTableVersion().versionCode(), Collections.emptyMap(), Collections.singletonMap(
            HoodieCommitMetadata.SCHEMA_KEY, SCHEMA_WITH_METADATA.toString()));
    testTable.addClean(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH), cleanerPlan, cleanMeta);
    startCommitTime += 1;

    // Clustering commit
    HoodieClusteringGroup group = new HoodieClusteringGroup();
    HoodieClusteringPlan plan = new HoodieClusteringPlan(Collections.singletonList(group),
        HoodieClusteringStrategy.newBuilder().build(), Collections.emptyMap(), 1, false, null);
    HoodieRequestedReplaceMetadata requestedMetadata = new HoodieRequestedReplaceMetadata(WriteOperationType.CLUSTER.name(), plan, Collections.emptyMap(), 1);
    testTable.addReplaceCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH), Option.of(requestedMetadata), Option.empty(),
            (HoodieReplaceCommitMetadata)buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UNKNOWN,
                SCHEMA_WITH_METADATA.toString(), CLUSTERING_ACTION));
    startCommitTime += 1;

    // Empty commits
    testTable.addCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH));
    startCommitTime += 1;

    testTable.addDeltaCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH));
    startCommitTime += 1;

    // non empty commits
    testTable.addCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        SCHEMA_WITH_METADATA.toString(),
        COMMIT_ACTION)));
    startCommitTime += 1;

    testTable.addDeltaCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH), buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        SCHEMA_WITH_METADATA.toString(),
        DELTA_COMMIT_ACTION));

    // Savepoint
    HoodieSavepointMetadata savepointMetadata = new HoodieSavepointMetadata();
    savepointMetadata.setSavepointedAt(12345L);
    savepointMetadata.setSavepointedBy("12345");
    savepointMetadata.setComments("12345");
    savepointMetadata.setPartitionMetadata(Collections.emptyMap());
    testTable.addSavepointCommit(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH), savepointMetadata);
    startCommitTime += 1;

    // Rollback
    testTable.addInflightRollback(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH));
    testTable.addRollback(padWithLeadingZeros(Integer.toString(startCommitTime), REQUEST_TIME_LENGTH), new HoodieRollbackMetadata(), new HoodieRollbackPlan());

    return startCommitTime;
  }

  @ParameterizedTest
  @MethodSource("commonTableConfigTestDimension")
  void testGetTableAvroSchemaInternalWithSpecificInstant(boolean preTableVersion8, HoodieTableType tableType) throws Exception {
    initMetaClient(preTableVersion8, tableType);
    testTable = HoodieTestTable.of(metaClient);
    createExhaustiveInstants(1, tableType);
  }
}
