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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.LogCapture;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
class TestEmbeddingColumnInfoRound extends EmbeddingRoundTestBase {
  @Test
  void successfulTableCommitPublishesAuxIdentityOutsideTheSchema() throws Exception {
    announce(text("first", "alpha beta"));
    String instant = round(compute()).getCommit().get().requestedTime();
    assertEquals(1, rows(table()).size());
    JsonNode info = info();
    assertEquals(1, info.get("layout_version").intValue());
    assertEquals(1, info.get("revision").intValue());
    assertEquals(1, info.get("events").size());
    JsonNode event = info.get("events").get(0);
    assertEquals("add", event.get("event").textValue());
    assertEquals(EmbeddingStreamers.MODEL_COLUMN, event.get("column_name").textValue());
    assertEquals(instant, event.get("instant").textValue());
    assertEquals(instant, event.get("added_at_instant").textValue());
    assertEquals("dummy", event.get("identity").get("provider").textValue());
    assertEquals("dummy", event.get("identity").get("model_name").textValue());
    assertEquals(8, event.get("identity").get("dimension").intValue());
    assertEquals(3, event.get("identity").size());
    assertEquals("1", meta().getTableConfig().getProps().getProperty("hoodie.table.embedding.column.info.layout.version"));
    org.apache.spark.sql.types.StructField vector = rows(table()).values().iterator().next().schema().apply(EmbeddingStreamers.MODEL_COLUMN);
    assertFalse(vector.getComment().isDefined());
    assertEquals("VECTOR(8)", vector.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));

    announce(text("second", "gamma delta"));
    assertTrue(round(compute()).getCommit().isPresent(), "same model is accepted");
    assertEquals(info, info(), "no new identity event or rewrite for the same model");
  }

  @Test
  void changedModelIsRefusedBeforePreparationAndTheCommitDoesNotAdvance() throws Exception {
    announce(text("first", "alpha beta"));
    round(compute());
    int before = EmbeddingStreamers.commits(table(), storage()).size();
    JsonNode original = info();
    announce(text("second", "gamma delta"));
    TypedProperties changed = compute();
    changed.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "another-model");
    RuntimeException failure = assertThrows(RuntimeException.class, () -> round(changed));
    assertTrue(messages(failure).contains(EmbeddingStreamers.MODEL_COLUMN), messages(failure));
    assertEquals(before, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(original, info());
    assertEquals(1, rows(table()).size());
  }

  @Test
  void missingHistoryWarnsRecoversAfterCommitAndValidatesTheNextRound() throws Exception {
    announce(text("first", "alpha beta"));
    round(compute());
    assertTrue(meta().getStorage().deleteFile(new StoragePath(table(), ".hoodie/.aux/embedding-column-info.json")));
    announce(text("second", "gamma delta"));
    try (LogCapture logs = LogCapture.of(EmbeddingColumnInfoStore.class)) {
      String instant = round(compute()).getCommit().get().requestedTime();
      assertEquals(1, logs.warnings().stream().filter(line -> line.contains(EmbeddingStreamers.MODEL_COLUMN)).count(), logs.lines().toString());
      JsonNode recovered = info().get("events").get(0);
      assertEquals("add", recovered.get("event").textValue());
      assertFalse(recovered.get("added_at_instant").isNull(), "the committed fence retains the actual creation instant");
      assertFalse(instant.equals(recovered.get("instant").textValue()), "recovery retains the earlier birth, not this round");
    }
    assertEquals(2, rows(table()).size());
    announce(text("third", "epsilon zeta"));
    TypedProperties changed = compute();
    changed.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "another-model");
    assertThrows(RuntimeException.class, () -> round(changed));
    assertEquals(2, rows(table()).size());
  }

  @Test
  void unknownTablePropertyLayoutRefusesAtStart() throws Exception {
    announce(text("first", "alpha beta"));
    round(compute());
    Properties properties = new Properties();
    properties.setProperty("hoodie.table.embedding.column.info.layout.version", "2");
    HoodieTableMetaClient meta = meta();
    HoodieTableConfig.update(meta.getStorage(), meta.getMetaPath(), properties);
    announce(text("second", "gamma delta"));
    TypedProperties unknownLayout = compute();
    RuntimeException failure = assertThrows(RuntimeException.class, () -> round(unknownLayout));
    assertTrue(messages(failure).contains("layout.version"), messages(failure));
    assertEquals(1, rows(table()).size());
  }

  @Test
  void modelChangeAddsANewColumnAndRetiresTheOldNameWithoutDroppingIt() throws Exception {
    announce(text("first", "alpha beta"));
    round(compute());
    announce(text("second", "gamma delta"));
    TypedProperties changed = compute();
    changed.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "new-model");
    changed.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_COLUMN_SUFFIX, "emb_new");
    changed.setProperty("hoodie.unstructured.embedding.retire.columns", EmbeddingStreamers.MODEL_COLUMN);
    String instant = round(changed).getCommit().get().requestedTime();
    JsonNode events = info().get("events");
    assertEquals(3, events.size());
    assertEquals("add", events.get(0).get("event").textValue());
    JsonNode retired = events.get(events.get(1).get("column_name").textValue().equals(EmbeddingStreamers.MODEL_COLUMN) ? 1 : 2);
    JsonNode added = events.get(events.get(1).get("column_name").textValue().equals("emb_new") ? 1 : 2);
    assertEquals("retire", retired.get("event").textValue());
    assertEquals("add", added.get("event").textValue());
    assertEquals(instant, retired.get("instant").textValue());
    assertEquals(instant, added.get("instant").textValue());
    assertEquals("new-model", added.get("identity").get("model_name").textValue());
    assertEquals(2, rows(table()).size());
    assertTrue(rows(table()).values().stream().allMatch(row -> row.schema().getFieldIndex(EmbeddingStreamers.MODEL_COLUMN).isDefined()));
    announce(text("third", "epsilon zeta"));
    JsonNode originalHistory = info();
    assertTrue(round(changed).getCommit().isPresent(), "the same streaming configuration can retain its retirement request");
    assertEquals(originalHistory, info(), "repeating the retirement does not append duplicate events");
    assertEquals(3, rows(table()).size());
    announce(text("fourth", "eta theta"));
    TypedProperties reusingRetired = compute();
    assertThrows(RuntimeException.class, () -> round(reusingRetired), "retired names cannot be reused");
    assertEquals(3, rows(table()).size());
  }

  @Test
  void publicationFailureFailsTheRoundAfterCommitAndKeepsCommittedData() throws Exception {
    try (LogCapture logs = LogCapture.of(EmbeddingTableTransformer.class)) {
      FailedPublication failed = failPublicationForNewColumn();
      assertTrue(messages(failed.failure).contains("embedding column information"), messages(failed.failure));
      assertEquals(2, EmbeddingStreamers.commits(table(), storage()).size(), "both table commits remain completed");
      assertEquals(0, meta().getActiveTimeline().getRollbackTimeline().countInstants(), "post-commit failure never rolls data back");
      assertEquals(2, rows(table()).size());
      assertArrayEquals(failed.previousHistory, Files.readAllBytes(auxPath()), "publication failure retains prior history bytes");
      assertTrue(rows(table()).values().stream().anyMatch(row -> row.getAs("emb_new") != null), "the new column data was committed");
      assertTrue(logs.lines().stream().anyMatch(line -> line.startsWith("ERROR ") && line.contains("publication")
          && line.contains("after successful commit") && line.contains(failed.creationInstant) && line.contains("retained")),
          "the error explicitly identifies publication after durable commit: " + logs.lines());
      Files.deleteIfExists(scratchPath());
    }
  }

  @Test
  void delayedAuxiliaryPublicationCannotAdmitAnotherModelIntoTheCommittedColumn() throws Exception {
    TypedProperties concurrent = compute();
    concurrent.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
    concurrent.setProperty("hoodie.write.lock.provider", "org.apache.hudi.core.transaction.lock.InProcessLockProvider");
    concurrent.setProperty("hoodie.cleaner.policy.failed.writes", "LAZY");
    FailedPublication failed = failPublicationForNewColumn(concurrent);
    int before = EmbeddingStreamers.commits(table(), storage()).size();
    assertEquals(2, before);
    announce(text("third", "epsilon zeta"));
    TypedProperties conflicting = TypedProperties.copy(failed.properties);
    conflicting.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_NAME_SUFFIX,
        "different-model");
    try {
      assertThrows(RuntimeException.class, () -> round(conflicting));
      assertEquals(before, EmbeddingStreamers.commits(table(), storage()).size(), "identity refusal precedes the data commit");
      assertEquals(2, rows(table()).size(), "no row from the conflicting model is durable");
    } finally {
      Files.deleteIfExists(scratchPath());
    }
  }

  @Test
  void committedColumnMissingFromHistoryWarnsAndAppendsItsOriginalAddEvent() throws Exception {
    FailedPublication failed = failPublicationForNewColumn();
    assertEquals(1, info().get("events").size(), "old history survives without the newly committed column");
    assertTrue(EmbeddingTableSchema.writerReadSchema(table(), storage()).get().getFieldIndex("emb_new").isDefined(),
        "the committed schema already has the column");
    Files.delete(scratchPath());
    announce(text("third", "epsilon zeta"));
    try (LogCapture logs = LogCapture.of(EmbeddingColumnInfoStore.class)) {
      assertTrue(round(failed.properties).getCommit().isPresent(), "the missing-information path continues the next round");
      assertEquals(1, logs.warnings().stream().filter(line -> line.contains("emb_new")).count(), logs.lines().toString());
    }
    JsonNode events = info().get("events");
    assertEquals(2, events.size());
    assertEquals("add", events.get(0).get("event").textValue(), "the old event stays intact");
    assertEquals("emb_new", events.get(1).get("column_name").textValue());
    assertEquals("add", events.get(1).get("event").textValue());
    assertEquals(failed.creationInstant, events.get(1).get("instant").textValue());
    assertEquals(failed.creationInstant, events.get(1).get("added_at_instant").textValue());
    assertEquals("new-model", events.get(1).get("identity").get("model_name").textValue());
    assertEquals(3, rows(table()).size());
    announce(text("fourth", "eta theta"));
    TypedProperties mismatch = TypedProperties.copy(failed.properties);
    mismatch.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "different-model");
    assertThrows(RuntimeException.class, () -> round(mismatch), "the recovered entry validates later rounds");
    assertEquals(3, rows(table()).size());
  }

  /** A real scratch-path obstruction changes neither the commit files nor the existing history file. */
  private FailedPublication failPublicationForNewColumn() throws Exception {
    return failPublicationForNewColumn(compute());
  }

  private FailedPublication failPublicationForNewColumn(TypedProperties firstProperties) throws Exception {
    announce(text("first", "alpha beta"));
    assertTrue(round(firstProperties).getCommit().isPresent());
    byte[] previousHistory = Files.readAllBytes(auxPath());
    Path scratch = scratchPath();
    try (java.util.stream.Stream<Path> entries = Files.list(scratch)) {
      assertEquals(0, entries.count(), "the writer cleaned its owned scratch");
    }
    Files.delete(scratch);
    Files.write(scratch, new byte[] {1});
    announce(text("second", "gamma delta"));
    TypedProperties changed = TypedProperties.copy(firstProperties);
    changed.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "new-model");
    changed.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_COLUMN_SUFFIX, "emb_new");
    RuntimeException failure = assertThrows(RuntimeException.class, () -> round(changed));
    List<org.apache.hudi.common.table.timeline.HoodieInstant> commits = EmbeddingStreamers.commits(table(), storage());
    assertEquals(2, commits.size(), "the table committed before auxiliary publication failed");
    return new FailedPublication(changed, commits.get(1).requestedTime(), failure, previousHistory);
  }

  private Path auxPath() {
    return localPath(table()).resolve(".hoodie/.aux/embedding-column-info.json");
  }

  private Path scratchPath() {
    return localPath(table()).resolve(".hoodie/.temp/embedding-column-info");
  }

  private static final class FailedPublication {
    private final TypedProperties properties;
    private final String creationInstant;
    private final RuntimeException failure;
    private final byte[] previousHistory;

    private FailedPublication(TypedProperties properties, String creationInstant, RuntimeException failure, byte[] previousHistory) {
      this.properties = properties;
      this.creationInstant = creationInstant;
      this.failure = failure;
      this.previousHistory = previousHistory;
    }
  }

  private TypedProperties compute() {
    return props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
  }

  private HoodieTableMetaClient meta() {
    return HoodieTableMetaClient.builder().setBasePath(table()).setConf(storage()).build();
  }

  private JsonNode info() throws Exception {
    try (InputStream in = meta().getStorage().open(new StoragePath(table(), ".hoodie/.aux/embedding-column-info.json"))) {
      return JsonUtils.getObjectMapper().readTree(in);
    }
  }

  private static String messages(Throwable failure) {
    StringBuilder text = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      text.append(cause.getMessage()).append('\n');
    }
    return text.toString();
  }
}
