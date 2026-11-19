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

import org.apache.hudi.client.timeline.LSMTimelineWriter;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.exception.HoodiePreCommitRefusalException;
import org.apache.hudi.utilities.streamer.CompletedWrite;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestEmbeddingColumnInfoStore {
  private static final String FIRST = "20261112090000000";
  private static final String SECOND = "20261112100000000";
  private static final String THIRD = "20261112110000000";
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final EmbeddingColumnInfoStore.Identity MODEL = identity("openai", "text-embedding-3-small", 1536, null);
  @TempDir
  Path temp;
  private String base;
  private StorageConfiguration<?> conf;
  private HoodieTableMetaClient meta;

  @BeforeEach
  void setUp() throws Exception {
    base = temp.resolve("table").toUri().toString();
    conf = HoodieTestUtils.getDefaultStorageConf();
    meta = HoodieTableMetaClient.newTableBuilder().setTableVersion(9).setTableName("column_info")
        .setTableType(HoodieTableType.COPY_ON_WRITE).initTable(conf, base);
  }

  @Test
  void lostHistoryWithoutAFenceRefusesEveryNewJob() throws Exception {
    add("text", MODEL, FIRST);
    Files.delete(file());
    for (int job = 0; job < 2; job++) {
      assertThrows(IllegalArgumentException.class,
          () -> store().checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536)));
    }
    assertFalse(Files.exists(file()));
  }

  @Test
  void noModelsLeavesBothPropertyAndFileAbsent() {
    EmbeddingColumnInfoStore store = store();
    store.checkAtStart(Collections.emptyMap(), Collections.emptyMap());
    assertFalse(Files.exists(file()));
    assertFalse(meta.getTableConfig().contains(EmbeddingColumnInfoStore.LAYOUT_VERSION));
  }

  @Test
  void firstSuccessfulCommitAddsTheExactReducedIdentityAndPinsLayout() throws Exception {
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap());
    assertFalse(Files.exists(file()), "start does not publish or reserve metadata");
    commit(FIRST);
    store.publishAfterCommit(FIRST, update);
    JsonNode snapshot = read();
    assertEquals(1, snapshot.path("layout_version").intValue());
    assertEquals(1, snapshot.path("revision").intValue());
    assertEquals(3, snapshot.size());
    JsonNode event = snapshot.path("events").get(0);
    assertEquals(1, event.path("sequence").intValue());
    assertEquals("text", event.path("column_name").textValue());
    assertEquals("add", event.path("event").textValue());
    assertEquals(FIRST, event.path("instant").textValue());
    assertEquals(FIRST, event.path("added_at_instant").textValue());
    assertEquals(6, event.size());
    assertEquals(JSON.readTree("{\"provider\":\"openai\",\"model_name\":\"text-embedding-3-small\",\"dimension\":1536}"), event.path("identity"));
    assertEquals("1", reload().getTableConfig().getString(EmbeddingColumnInfoStore.LAYOUT_VERSION));
    assertFalse(Files.exists(file().resolveSibling(".embedding-column-info.json.crc")), "aux history has no checksum sidecar");
  }

  @Test
  void incompleteInstantNeverPublishesMetadata() {
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap());
    assertThrows(IllegalArgumentException.class, () -> store.publishAfterCommit(FIRST, update));
    assertFalse(Files.exists(file()));
    assertFalse(reload().getTableConfig().contains(EmbeddingColumnInfoStore.LAYOUT_VERSION));
  }

  @Test
  void sameIdentityAndRepeatedPublicationAreIdempotent() throws Exception {
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap());
    commit(FIRST);
    store.publishAfterCommit(FIRST, update);
    byte[] before = Files.readAllBytes(file());
    store.publishAfterCommit(FIRST, update);
    commit(SECOND);
    store.publishAfterCommit(SECOND, store.checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536)));
    assertArrayEquals(before, Files.readAllBytes(file()));
  }

  @Test
  void anyPresentIdentityMismatchRefusesTheSamePhysicalColumn() throws Exception {
    add("text", MODEL, FIRST);
    for (EmbeddingColumnInfoStore.Identity changed : Arrays.asList(identity("bedrock", "text-embedding-3-small", 1536, null),
        identity("openai", "another", 1536, null), identity("openai", "text-embedding-3-small", 3072, null),
        identity("openai", "text-embedding-3-small", 1536, "r1"))) {
      assertThrows(IllegalArgumentException.class,
          () -> store().checkAtStart(Collections.singletonMap("text", changed), Collections.singletonMap("text", 1536)));
    }
  }

  @Test
  void missingEntryRecoveryRetainsOtherHistoryAndDoesNotInventBirth() throws Exception {
    add("old", MODEL, FIRST);
    EmbeddingColumnInfoStore.Identity recovered = identity("bedrock", "amazon.titan-embed-image-v1", 1024, null);
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("image", recovered),
        Collections.singletonMap("image", 1024));
    commit(SECOND);
    store.publishAfterCommit(SECOND, update);
    JsonNode snapshot = read();
    assertEquals(2, snapshot.path("revision").intValue());
    assertEquals(2, snapshot.path("events").size());
    assertEquals("old", snapshot.path("events").get(0).path("column_name").textValue());
    JsonNode recoveredEvent = snapshot.path("events").get(1);
    assertEquals("recover", recoveredEvent.path("event").textValue());
    assertTrue(recoveredEvent.path("added_at_instant").isNull());
    assertEquals(SECOND, recoveredEvent.path("instant").textValue());
  }

  @Test
  void failedPublicationWithSurvivingHistoryAddsTheColumnAtItsActualCommittedBirth() throws Exception {
    EmbeddingColumnInfoStore initial = store();
    EmbeddingColumnInfoStore.Update old = initial.checkAtStart(Collections.singletonMap("old", MODEL), Collections.emptyMap());
    commit(FIRST, Collections.singletonMap("old", 1536));
    initial.publishAfterCommit(FIRST, old);
    byte[] before = Files.readAllBytes(file());
    JsonNode oldEvent = read().path("events").get(0);
    EmbeddingColumnInfoStore.Identity newer = identity("openai", "text-embedding-3-large", 3072, null);
    EmbeddingColumnInfoStore failing = new EmbeddingColumnInfoStore(base, conf, new TypedProperties(), new EmbeddingColumnInfoAtomicSwap() {
      @Override
      void replace(HoodieStorage storage, StoragePath scratch, StoragePath target) throws IOException {
        throw new IOException("injected post-commit publication failure");
      }
    });
    EmbeddingColumnInfoStore.Update adding = failing.checkAtStart(Collections.singletonMap("new", newer), Collections.singletonMap("old", 1536));
    Map<String, Integer> committedVectors = new java.util.TreeMap<>();
    committedVectors.put("old", 1536);
    committedVectors.put("new", 3072);
    commit(SECOND, committedVectors);
    assertThrows(HoodieIOException.class, () -> failing.publishAfterCommit(SECOND, adding));
    assertArrayEquals(before, Files.readAllBytes(file()), "failed publication preserves the earlier history snapshot");

    EmbeddingColumnInfoStore nextRound = store();
    EmbeddingColumnInfoStore.Update repair = nextRound.checkAtStart(Collections.singletonMap("new", newer), committedVectors);
    assertArrayEquals(before, Files.readAllBytes(file()), "start captures provenance without writing metadata");
    String third = "20261112110000000";
    commit(third, committedVectors);
    nextRound.publishAfterCommit(third, repair);
    JsonNode snapshot = read();
    assertEquals(2, snapshot.path("revision").intValue());
    assertEquals(2, snapshot.path("events").size());
    assertEquals(oldEvent, snapshot.path("events").get(0));
    JsonNode added = snapshot.path("events").get(1);
    assertEquals("add", added.path("event").textValue());
    assertEquals(SECOND, added.path("instant").textValue(), "add belongs to the original successful table commit");
    assertEquals(SECOND, added.path("added_at_instant").textValue());
    assertEquals("new", added.path("column_name").textValue());
    assertEquals("text-embedding-3-large", added.path("identity").path("model_name").textValue());
    assertEquals(3072, added.path("identity").path("dimension").intValue());
    assertEquals(6, added.size());
    assertEquals(3, added.path("identity").size());
    byte[] repaired = Files.readAllBytes(file());
    nextRound.publishAfterCommit(third, repair);
    assertArrayEquals(repaired, Files.readAllBytes(file()), "publication retry retains a single ADD");
    assertThrows(IllegalArgumentException.class, () -> store().checkAtStart(
        Collections.singletonMap("new", identity("openai", "different-model", 3072, null)), committedVectors));
  }

  @Test
  void firstRetainedContainingSchemaDoesNotInventAColumnBirth() throws Exception {
    add("old", MODEL, FIRST);
    Map<String, Integer> vectors = new java.util.TreeMap<>();
    vectors.put("old", 1536);
    vectors.put("new", 3072);
    commit(SECOND, vectors);
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(
        Collections.singletonMap("new", identity("openai", "new-model", 3072, null)), vectors);
    String third = "20261112110000000";
    commit(third, vectors);
    store.publishAfterCommit(third, update);
    JsonNode recovered = read().path("events").get(1);
    assertEquals("recover", recovered.path("event").textValue());
    assertTrue(recovered.path("added_at_instant").isNull(), "no preceding committed schema proves when the column first appeared");
  }

  @Test
  void schemaMissingFromAnInterveningCommitDoesNotInventAColumnBirth() throws Exception {
    EmbeddingColumnInfoStore initial = store();
    EmbeddingColumnInfoStore.Update old = initial.checkAtStart(Collections.singletonMap("old", MODEL), Collections.emptyMap());
    commit(FIRST, Collections.singletonMap("old", 1536));
    initial.publishAfterCommit(FIRST, old);
    commit(SECOND);
    Map<String, Integer> vectors = new java.util.TreeMap<>();
    vectors.put("old", 1536);
    vectors.put("new", 3072);
    String third = "20261112110000000";
    commit(third, vectors);
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(
        Collections.singletonMap("new", identity("openai", "new-model", 3072, null)), vectors);
    String fourth = "20261112120000000";
    commit(fourth, vectors);
    store.publishAfterCommit(fourth, update);
    JsonNode recovered = read().path("events").get(1);
    assertEquals("recover", recovered.path("event").textValue());
    assertTrue(recovered.path("added_at_instant").isNull(), "the column may have been created by the intervening schema-less commit");
  }

  @Test
  void archivedCreationBetweenRetainedSchemasDoesNotInventALaterBirth() throws Exception {
    EmbeddingColumnInfoStore initial = store();
    EmbeddingColumnInfoStore.Update old = initial.checkAtStart(Collections.singletonMap("old", MODEL), Collections.emptyMap());
    commit(FIRST, Collections.singletonMap("old", 1536));
    initial.publishAfterCommit(FIRST, old);
    Map<String, Integer> vectors = new java.util.TreeMap<>();
    vectors.put("old", 1536);
    vectors.put("new", 3072);
    commit(SECOND, vectors);
    HoodieInstant creation = meta.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream().filter(instant -> SECOND.equals(instant.requestedTime())).findFirst().get();
    HoodieWriteConfig archivalConfig = HoodieWriteConfig.newBuilder().withPath(base).forTable("column_info").build();
    LSMTimelineWriter.getInstance(archivalConfig, new LocalTaskContextSupplier(), meta).write(
        Collections.singletonList(ActiveAction.fromInstants(Collections.singletonList(creation))), Option.empty(),
        Option.of(failure -> {
          throw new IllegalStateException("cannot prepare archived creation fixture", failure);
        }));
    meta.getActiveTimeline().deleteInstantFileIfExists(creation);
    String third = "20261112110000000";
    commit(third, vectors);
    assertTrue(meta.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().containsInstant(FIRST));
    assertFalse(meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().containsInstant(SECOND));
    assertTrue(meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().containsInstant(third));
    assertTrue(reload().getArchivedTimeline().containsInstant(SECOND), "actual creation is archived between retained schemas");

    EmbeddingColumnInfoStore nextRound = store();
    EmbeddingColumnInfoStore.Update update = nextRound.checkAtStart(
        Collections.singletonMap("new", identity("openai", "new-model", 3072, null)), vectors);
    String fourth = "20261112120000000";
    commit(fourth, vectors);
    nextRound.publishAfterCommit(fourth, update);
    JsonNode recovered = read().path("events").get(1);
    assertEquals("recover", recovered.path("event").textValue(), "retained absent/containing schemas do not prove adjacent commits");
    assertTrue(recovered.path("added_at_instant").isNull(), "never substitute the later retained containing instant for an archived birth");
  }

  @Test
  void missingFileRecoveryStillChecksVectorDimensions() throws Exception {
    pin("1");
    assertThrows(IllegalArgumentException.class,
        () -> store().checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 8)));
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536));
    commit(FIRST);
    store.publishAfterCommit(FIRST, update);
    assertEquals("recover", read().path("events").get(0).path("event").textValue());
    assertTrue(read().path("events").get(0).path("added_at_instant").isNull());
  }

  @Test
  void modelChangeUsesNewColumnAndRetainsRetirementHistory() throws Exception {
    add("old", MODEL, FIRST);
    EmbeddingColumnInfoStore.Identity newer = identity("openai", "text-embedding-3-large", 3072, "r2");
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("new", newer),
        Collections.singletonMap("old", 1536), Collections.singleton("old"));
    commit(SECOND);
    store.publishAfterCommit(SECOND, update);
    JsonNode events = read().path("events");
    assertEquals(3, events.size());
    assertEquals("retire", events.get(1).path("event").textValue());
    assertEquals(FIRST, events.get(1).path("added_at_instant").textValue());
    assertEquals(events.get(0).path("identity"), events.get(1).path("identity"));
    assertEquals("add", events.get(2).path("event").textValue());
    assertEquals("r2", events.get(2).path("identity").path("model_version").textValue());
    assertThrows(IllegalArgumentException.class,
        () -> store().checkAtStart(Collections.singletonMap("old", MODEL), Collections.singletonMap("old", 1536)));
  }

  @Test
  void repeatedRetirementConfigurationAcrossRoundsIsIdempotent() throws Exception {
    add("old", MODEL, FIRST);
    EmbeddingColumnInfoStore.Identity newer = identity("openai", "text-embedding-3-large", 3072, "r2");
    Map<String, EmbeddingColumnInfoStore.Identity> configured = Collections.singletonMap("new", newer);
    Map<String, Integer> existing = Collections.singletonMap("old", 1536);
    java.util.Set<String> retired = Collections.singleton("old");
    EmbeddingColumnInfoStore firstRound = store();
    EmbeddingColumnInfoStore.Update transition = firstRound.checkAtStart(configured, existing, retired);
    commit(SECOND);
    firstRound.publishAfterCommit(SECOND, transition);
    byte[] before = Files.readAllBytes(file());
    assertEquals(2, read().path("revision").intValue());
    assertEquals(3, read().path("events").size());
    firstRound.publishAfterCommit(SECOND, transition);
    assertArrayEquals(before, Files.readAllBytes(file()), "repeating an applied Update is already idempotent");

    EmbeddingColumnInfoStore nextRound = store();
    EmbeddingColumnInfoStore.Update unchanged = nextRound.checkAtStart(configured, existing, retired);
    String third = "20261112110000000";
    commit(third);
    nextRound.publishAfterCommit(third, unchanged);
    assertArrayEquals(before, Files.readAllBytes(file()), "a persistent retirement setting does not append or rewrite history");
    assertEquals(2, read().path("revision").intValue());
    assertEquals(3, read().path("events").size());
    assertThrows(IllegalArgumentException.class,
        () -> store().checkAtStart(Collections.singletonMap("old", MODEL), existing), "configured retired-name reuse still refuses");
  }

  @Test
  void invalidRetirementRequestsNeverChangeHistory() throws Exception {
    add("old", MODEL, FIRST);
    byte[] before = Files.readAllBytes(file());
    assertThrows(IllegalArgumentException.class, () -> store().checkAtStart(Collections.singletonMap("old", MODEL),
        Collections.singletonMap("old", 1536), Collections.singleton("old")));
    assertThrows(IllegalArgumentException.class, () -> store().checkAtStart(Collections.emptyMap(),
        Collections.singletonMap("old", 1536), Collections.singleton("unknown")));
    assertArrayEquals(before, Files.readAllBytes(file()));
  }

  @Test
  void unsupportedLayoutRefusesBeforeParsingAndUngovernedFileRefuses() throws Exception {
    pin("2");
    raw("not JSON");
    IllegalArgumentException unsupported = assertThrows(IllegalArgumentException.class,
        () -> store().checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536)));
    assertTrue(unsupported.getMessage().contains("layout"), unsupported.getMessage());
    HoodieTableConfig.delete(meta.getStorage(), meta.getMetaPath(), Collections.singleton(EmbeddingColumnInfoStore.LAYOUT_VERSION.key()));
    assertThrows(IllegalArgumentException.class,
        () -> store().checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536)));
  }

  @Test
  void malformedTypesDuplicateKeysUnknownFieldsAndInvalidHistoryRefuse() throws Exception {
    pin("1");
    String valid = "{\"layout_version\":1,\"revision\":1,\"events\":[{\"sequence\":1,\"column_name\":\"text\","
        + "\"event\":\"add\",\"instant\":\"" + FIRST + "\",\"added_at_instant\":\"" + FIRST + "\","
        + "\"identity\":{\"provider\":\"openai\",\"model_name\":\"text-embedding-3-small\",\"dimension\":1536}}]}";
    for (String malformed : Arrays.asList(valid.replace("\"layout_version\":1", "\"layout_version\":2"),
        valid.replace("\"revision\":1", "\"revision\":\"1\""), valid.replace("\"revision\":1", "\"revision\":0"),
        valid.replace("\"revision\":1", "\"revision\":1,\"revision\":2"),
        valid.replace("\"events\":", "\"unknown\":true,\"events\":"),
        valid.replace("\"dimension\":1536", "\"dimension\":1536.0"),
        valid.replace("\"dimension\":1536", "\"dimension\":1536,\"endpoint\":\"secret\""),
        valid.replace("\"event\":\"add\"", "\"event\":\"migrate\""),
        valid.replace("\"sequence\":1", "\"sequence\":0"),
        valid.replace("\"added_at_instant\":\"" + FIRST + "\"", "\"added_at_instant\":null"),
        valid + " {}")) {
      raw(malformed);
      assertThrows(IllegalArgumentException.class,
          () -> store().checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536)), malformed);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void committedIdentityFencesStaleWritersBeforeAuxiliaryPublication(boolean sameColumn) throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
    props.setProperty(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), InProcessLockProvider.class.getName());
    EmbeddingColumnInfoStore first = new EmbeddingColumnInfoStore(base, conf, props);
    EmbeddingColumnInfoStore second = new EmbeddingColumnInfoStore(base, conf, props);
    Map<String, EmbeddingColumnInfoStore.Identity> a = Collections.singletonMap("text", MODEL);
    Map<String, EmbeddingColumnInfoStore.Identity> b = Collections.singletonMap(sameColumn ? "text" : "other",
        identity("openai", "another-model", 1536, null));
    first.checkAtStart(a, Collections.emptyMap());
    second.checkAtStart(b, Collections.emptyMap());
    HoodieCommitMetadata committed = new HoodieCommitMetadata();
    // The existing strict layout1 snapshot is the committed identity contract, independent of auxiliary publication.
    String snapshot = "{\"layout_version\":1,\"revision\":1,\"events\":[{\"sequence\":1,\"column_name\":\"text\","
            + "\"event\":\"add\",\"instant\":\"" + FIRST + "\",\"added_at_instant\":\"" + FIRST + "\","
            + "\"identity\":{\"provider\":\"openai\",\"model_name\":\"text-embedding-3-small\",\"dimension\":1536}}]}";
    Path fence = temp.resolve("table/.hoodie/.aux/embedding-column-fence.json");
    Files.createDirectories(fence.getParent());
    Files.write(fence, ("{\"layout_version\":1,\"committed\":null,\"pending\":{\"instant\":\"" + FIRST
        + "\",\"snapshot\":" + snapshot + "}}").getBytes(StandardCharsets.UTF_8));
    commit(FIRST, committed);
    assertFalse(Files.exists(file()), "the first writer has not published auxiliary history yet");
    if (sameColumn) {
      IllegalArgumentException conflict = assertThrows(IllegalArgumentException.class,
          () -> second.checkAtStart(b, Collections.emptyMap()));
      assertTrue(conflict.getMessage().contains("different model identity"), conflict.getMessage());
      assertEquals(1, reload().getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
    } else {
      second.checkAtStart(b, Collections.emptyMap());
      commit(SECOND);
      assertEquals(2, reload().getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void staleWritersAreRecheckedInsideTheDataCommitTransaction(boolean sameColumn) throws Exception {
    EmbeddingColumnInfoStore first = store();
    EmbeddingColumnInfoStore second = store();
    EmbeddingColumnInfoStore.Update a = first.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap());
    EmbeddingColumnInfoStore.Update b = second.checkAtStart(Collections.singletonMap(sameColumn ? "text" : "other",
        identity("openai", "another-model", 1536, null)), Collections.emptyMap());
    commitFenced(first, a, FIRST, true);
    assertFalse(Files.exists(file()), "publication remains delayed after the first data commit");
    if (sameColumn) {
      HoodiePreCommitRefusalException failure = assertThrows(HoodiePreCommitRefusalException.class, () -> commitFenced(second, b, SECOND, true));
      assertTrue(failure.getMessage().contains("different model identity"), failure.getMessage());
      assertEquals(1, reload().getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
    } else {
      commitFenced(second, b, SECOND, true);
      second.publishCommittedState(SECOND, b);
      assertEquals(2, read().path("events").size());
      byte[] before = Files.readAllBytes(file());
      first.publishCommittedState(FIRST, a);
      assertArrayEquals(before, Files.readAllBytes(file()), "a delayed callback preserves the newer committed state");
      assertEquals(2, reload().getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
    }
  }

  @Test
  void uncompletedCandidateDoesNotReserveTheColumnAfterCommitFailure() throws Exception {
    EmbeddingColumnInfoStore first = store();
    EmbeddingColumnInfoStore second = store();
    EmbeddingColumnInfoStore.Update a = first.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap());
    EmbeddingColumnInfoStore.Update b = second.checkAtStart(Collections.singletonMap("text",
        identity("openai", "another-model", 1536, null)), Collections.emptyMap());
    commitFenced(first, a, FIRST, false);
    commitFenced(second, b, SECOND, true);
    second.publishCommittedState(SECOND, b);
    assertEquals("another-model", read().path("events").get(0).path("identity").path("model_name").textValue());
    assertThrows(HoodiePreCommitRefusalException.class, () -> commitFenced(first, a, "20261112110000000", true));
    assertEquals(1, reload().getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
  }

  @Test
  void retirementFencesAnAlreadyStartedWriterBeforeItsCommit() throws Exception {
    EmbeddingColumnInfoStore seed = store();
    EmbeddingColumnInfoStore.Update add = seed.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap());
    commitFenced(seed, add, FIRST, true);
    seed.publishCommittedState(FIRST, add);
    EmbeddingColumnInfoStore writer = store();
    EmbeddingColumnInfoStore.Update stale = writer.checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536));
    EmbeddingColumnInfoStore retirement = store();
    EmbeddingColumnInfoStore.Update retire = retirement.checkAtStart(Collections.emptyMap(), Collections.singletonMap("text", 1536),
        Collections.singleton("text"));
    commitFenced(retirement, retire, SECOND, true);
    HoodiePreCommitRefusalException failure = assertThrows(HoodiePreCommitRefusalException.class,
        () -> commitFenced(writer, stale, "20261112110000000", true));
    assertTrue(failure.getMessage().contains("retired"), failure.getMessage());
    seed.publishCommittedState(FIRST, add);
    assertEquals("retire", read().path("events").get(1).path("event").textValue());
    assertEquals(2, reload().getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants());
  }

  @Test
  void archivedPendingCommitStillFencesAdmissionAndPublishesItsDelayedCallback() throws Exception {
    EmbeddingColumnInfoStore first = store();
    EmbeddingColumnInfoStore second = store();
    EmbeddingColumnInfoStore.Update a = first.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap());
    EmbeddingColumnInfoStore.Update b = second.checkAtStart(Collections.singletonMap("text",
        identity("openai", "another-model", 1536, null)), Collections.emptyMap());
    commitFenced(first, a, FIRST, true);
    HoodieInstant archived = meta.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().firstInstant().get();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(base).forTable("column_info").build();
    LSMTimelineWriter.getInstance(config, new LocalTaskContextSupplier(), meta).write(
        Collections.singletonList(ActiveAction.fromInstants(Collections.singletonList(archived))), Option.empty(),
        Option.of(failure -> {
          throw new IllegalStateException("cannot prepare archive fixture", failure);
        }));
    // as the archiver does: every state file of the archived instant leaves the active timeline
    meta.getActiveTimeline().deleteInstantFileIfExists(archived);
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, FIRST));
    assertFalse(meta.reloadActiveTimeline().getCommitsTimeline().containsInstant(FIRST));
    assertTrue(CompletedWrite.exists(reload(), FIRST));
    assertFalse(CompletedWrite.exists(reload(), SECOND), "a skipped or uncompleted write is never published");
    assertThrows(HoodiePreCommitRefusalException.class, () -> commitFenced(second, b, SECOND, true));
    first.publishCommittedState(FIRST, a);
    assertEquals("text", read().path("events").get(0).path("column_name").textValue());
  }

  /**
   * The fence's candidate stays committed authority after its instant was archived behind a savepointed commit (archive beyond
   * savepoint can archive every commit that is not savepointed) and that savepoint was deleted: another model for the column is
   * still refused.
   */
  @Test
  void candidateArchivedBehindADeletedSavepointStillFencesAdmission() throws Exception {
    commit(FIRST);
    commit(SECOND);
    EmbeddingColumnInfoStore first = store();
    Map<String, Integer> noVectors = Collections.emptyMap();
    EmbeddingColumnInfoStore.Update a = first.checkAtStart(Collections.singletonMap("text", MODEL), noVectors);
    commitFenced(first, a, THIRD, true);
    savepoint(SECOND);
    HoodieInstant archived = meta.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    assertEquals(THIRD, archived.requestedTime());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(base).forTable("column_info").build();
    LSMTimelineWriter.getInstance(config, new LocalTaskContextSupplier(), meta).write(
        Collections.singletonList(ActiveAction.fromInstants(Collections.singletonList(archived))), Option.empty(),
        Option.of(failure -> {
          throw new IllegalStateException("cannot prepare archive fixture", failure);
        }));
    meta.getActiveTimeline().deleteInstantFileIfExists(archived);
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, THIRD));
    // the operator deletes the savepoint: the newest active commit is no longer savepointed
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.reloadActiveTimeline().getSavePointTimeline().filterCompletedInstants().firstInstant().get());
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION, SECOND));
    assertTrue(reload().getActiveTimeline().getSavePointTimeline().empty());
    assertFalse(Files.exists(file()), "the candidate was never published to the history");

    EmbeddingColumnInfoStore next = store();
    Map<String, EmbeddingColumnInfoStore.Identity> another = Collections.singletonMap("text", identity("openai", "another-model", 1536, null));
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> next.checkAtStart(another, noVectors));
    assertTrue(refused.getMessage().contains("different model identity"), refused.getMessage());
  }

  private void savepoint(String instant) {
    HoodieInstant inflight = meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION, instant);
    meta.reloadActiveTimeline().createNewInstant(inflight);
    meta.getActiveTimeline().saveAsComplete(inflight,
        Option.of(TimelineMetadataUtils.convertSavepointMetadata("test", "test", Collections.emptyMap())));
  }

  /** A manual publication on a fenced table joins the fence, which keeps it even after the history file is lost. */
  @Test
  void manualPublicationOnAFencedTableRetainsOneCommittedAuthority() throws Exception {
    EmbeddingColumnInfoStore first = store();
    EmbeddingColumnInfoStore.Update a = first.checkAtStart(Collections.singletonMap("a", MODEL), Collections.emptyMap());
    commitFenced(first, a, FIRST, true);
    first.publishCommittedState(FIRST, a);
    EmbeddingColumnInfoStore manual = store();
    EmbeddingColumnInfoStore.Update c = manual.checkAtStart(Collections.singletonMap("c", MODEL), Collections.emptyMap());
    commit(SECOND);
    manual.publishAfterCommit(SECOND, c);
    assertEquals(2, read().path("events").size());
    EmbeddingColumnInfoStore next = store();
    assertThrows(IllegalArgumentException.class, () -> next.checkAtStart(Collections.singletonMap("c",
        identity("openai", "another-model", 1536, null)), Collections.emptyMap()));
    Files.delete(file());
    first.publishCommittedState(FIRST, a);
    assertEquals(2, read().path("events").size(), "the fence retains the manually published column even after auxiliary loss");
    assertThrows(IllegalArgumentException.class, () -> next.checkAtStart(Collections.singletonMap("c",
        identity("openai", "another-model", 1536, null)), Collections.emptyMap()));
  }

  /**
   * An instant archived inside the active window (an archiver that archives beyond a savepoint keeps the savepointed commit and
   * archives later ones) is still a completed write: CompletedWrite reads the archive for every instant absent from the active
   * timeline.
   */
  @Test
  void instantArchivedInsideTheActiveWindowIsStillACompletedWrite() throws Exception {
    commit(FIRST);
    commit(SECOND);
    commit(THIRD);
    HoodieInstant archived = meta.reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(instant -> instant.requestedTime().equals(SECOND)).findFirst().get();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(base).forTable("column_info").build();
    LSMTimelineWriter.getInstance(config, new LocalTaskContextSupplier(), meta).write(
        Collections.singletonList(ActiveAction.fromInstants(Collections.singletonList(archived))), Option.empty(),
        Option.of(failure -> {
          throw new IllegalStateException("cannot prepare archive fixture", failure);
        }));
    meta.getActiveTimeline().deleteInstantFileIfExists(archived);
    meta.getActiveTimeline().deleteInstantFileIfExists(
        meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, SECOND));
    HoodieTimeline active = reload().getActiveTimeline().getCommitsTimeline();
    assertTrue(active.containsInstant(FIRST) && active.containsInstant(THIRD) && !active.containsInstant(SECOND),
        "the archived instant leaves a hole between two active ones");
    assertTrue(CompletedWrite.exists(reload(), SECOND), "the instant archived in the hole is found in the archive");
    assertFalse(CompletedWrite.exists(reload(), "20261112103000000"), "an instant that never existed is not");
  }

  @Test
  void emptyManualPublicationKeepsTheFenceReadable() throws Exception {
    EmbeddingColumnInfoStore first = store();
    Map<String, Integer> noVectors = Collections.emptyMap();
    EmbeddingColumnInfoStore.Update a = first.checkAtStart(Collections.singletonMap("text", MODEL), noVectors);
    commitFenced(first, a, FIRST, false);
    Path fence = temp.resolve("table").resolve(EmbeddingColumnInfoStore.FENCE_PATH);
    byte[] originalFence = Files.readAllBytes(fence);
    EmbeddingColumnInfoStore manual = store();
    EmbeddingColumnInfoStore.Update nothing = manual.checkAtStart(Collections.emptyMap(), noVectors);
    commit(SECOND);
    manual.publishAfterCommit(SECOND, nothing);
    EmbeddingColumnInfoStore next = store();
    Map<String, EmbeddingColumnInfoStore.Identity> another = Collections.singletonMap("text", identity("openai", "another-model", 1536, null));
    EmbeddingColumnInfoStore.Update admitted = next.checkAtStart(another, noVectors);
    assertArrayEquals(originalFence, Files.readAllBytes(fence), "an empty publication keeps the valid fence unchanged");
    assertFalse(Files.exists(file()), "nothing has a committed identity to publish yet");
    assertFalse(reload().getTableConfig().contains(EmbeddingColumnInfoStore.LAYOUT_VERSION));
    commitFenced(next, admitted, THIRD, true);
    next.publishCommittedState(THIRD, admitted);
    assertEquals("another-model", read().path("events").get(0).path("identity").path("model_name").textValue(),
        "the failed candidate reserved nothing and the fence still reads");
  }

  /** A completed publication with no models keeps the optional history absent. */
  @Test
  void completedEmptyManualPublicationLeavesHistoryAbsent() throws Exception {
    EmbeddingColumnInfoStore manual = store();
    EmbeddingColumnInfoStore.Update nothing = manual.checkAtStart(Collections.emptyMap(), Collections.emptyMap());
    commit(FIRST);
    manual.publishAfterCommit(FIRST, nothing);
    assertFalse(Files.exists(file()));
    assertFalse(Files.exists(temp.resolve("table").resolve(EmbeddingColumnInfoStore.FENCE_PATH)));
    assertFalse(reload().getTableConfig().contains(EmbeddingColumnInfoStore.LAYOUT_VERSION));
  }

  /** Coexisting valid legacy and fenced histories retain all identities and never lower a published revision. */
  @ParameterizedTest(name = "publishedNewer={0}")
  @ValueSource(booleans = {false, true})
  void mergedHistoriesKeepRevisionAndRepeatedCallbacksStable(boolean publishedNewer) throws Exception {
    String fourth = "20261112120000000";
    String[] instants = {FIRST, SECOND, THIRD, fourth};
    String[] authorityColumns = publishedNewer ? new String[] {"a"} : new String[] {"a", "c", "d"};
    EmbeddingColumnInfoStore.Update firstUpdate = null;
    for (int index = 0; index < authorityColumns.length; index++) {
      EmbeddingColumnInfoStore authorityStore = store();
      EmbeddingColumnInfoStore.Update update = authorityStore.checkAtStart(
          Collections.singletonMap(authorityColumns[index], MODEL), Collections.emptyMap());
      if (index == 0) {
        firstUpdate = update;
      }
      commitFenced(authorityStore, update, instants[index], true);
      authorityStore.publishCommittedState(instants[index], update);
    }
    JsonNode authority = read();
    Path fence = temp.resolve("table").resolve(EmbeddingColumnInfoStore.FENCE_PATH);
    byte[] authoritativeFence = Files.readAllBytes(fence);
    // Prepare independent, valid legacy history in this isolated fixture; then restore the real fence before the call.
    Files.delete(fence);
    Files.delete(file());
    String[] publishedColumns = publishedNewer ? new String[] {"b", "c", "d"} : new String[] {"b"};
    for (int index = 0; index < publishedColumns.length; index++) {
      add(publishedColumns[index], MODEL, instants[authorityColumns.length + index]);
    }
    JsonNode published = read();
    Files.write(fence, authoritativeFence);
    com.fasterxml.jackson.databind.node.ArrayNode expectedEvents = authority.path("events").deepCopy();
    for (JsonNode event : published.path("events")) {
      com.fasterxml.jackson.databind.node.ObjectNode copied = event.deepCopy();
      copied.put("sequence", expectedEvents.size() + 1);
      expectedEvents.add(copied);
    }
    store().publishCommittedState(FIRST, firstUpdate);
    assertEquals(expectedEvents, read().path("events"), "all identity, birth and commit fields survive reconciliation");
    assertEquals(4, read().path("revision").intValue(), "the changed merge is above both valid input revisions");
    assertArrayEquals(authoritativeFence, Files.readAllBytes(fence), "canonical publication preserves fence authority");
    byte[] merged = Files.readAllBytes(file());
    store().publishCommittedState(FIRST, firstUpdate);
    assertArrayEquals(merged, Files.readAllBytes(file()), "an identical delayed callback changes no bytes or revision");
    store().publishCommittedState(FIRST, firstUpdate);
    assertArrayEquals(merged, Files.readAllBytes(file()), "repeated callbacks remain idempotent");
  }

  private void commitFenced(EmbeddingColumnInfoStore store, EmbeddingColumnInfoStore.Update update, String instant, boolean complete) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
    props.setProperty(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), InProcessLockProvider.class.getName());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(props).withPath(base).forTable("column_info").build();
    HoodieInstant action = meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant);
    try (TransactionManager transaction = new TransactionManager(config, meta.getStorage())) {
      assertTrue(transaction.isLockRequired(), "the test exercises the table transaction lock");
      transaction.beginStateChange(Option.of(action), Option.empty());
      try {
        store.validateBeforeCommit(meta, instant, update);
        if (complete) {
          commit(instant);
        }
      } finally {
        transaction.endStateChange(Option.of(action));
      }
    }
  }

  @Test
  void staleDistinctColumnUpdatesRereadAndRetainBothHistories() throws Exception {
    EmbeddingColumnInfoStore first = store();
    EmbeddingColumnInfoStore second = store();
    EmbeddingColumnInfoStore.Update a = first.checkAtStart(Collections.singletonMap("a", MODEL), Collections.emptyMap());
    EmbeddingColumnInfoStore.Update b = second.checkAtStart(Collections.singletonMap("b", MODEL), Collections.emptyMap());
    commit(FIRST);
    commit(SECOND);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> x = pool.submit(() -> first.publishAfterCommit(FIRST, a));
      Future<?> y = pool.submit(() -> second.publishAfterCommit(SECOND, b));
      x.get();
      y.get();
    } finally {
      pool.shutdownNow();
    }
    JsonNode snapshot = read();
    assertEquals(2, snapshot.path("revision").intValue());
    assertEquals(2, snapshot.path("events").size());
    java.util.Set<String> columns = new java.util.HashSet<>();
    snapshot.path("events").forEach(event -> columns.add(event.path("column_name").textValue()));
    assertEquals(new java.util.HashSet<>(Arrays.asList("a", "b")), columns);
  }

  @Test
  void failedSwapPreservesCompleteOldBytesAndCleansOwnedScratch() throws Exception {
    add("old", MODEL, FIRST);
    byte[] before = Files.readAllBytes(file());
    EmbeddingColumnInfoStore store = new EmbeddingColumnInfoStore(base, conf, new TypedProperties(), new EmbeddingColumnInfoAtomicSwap() {
      @Override
      void replace(HoodieStorage storage, StoragePath scratch, StoragePath target) throws IOException {
        throw new IOException("injected swap failure");
      }
    });
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("new", MODEL), Collections.singletonMap("old", 1536));
    commit(SECOND);
    assertThrows(HoodieIOException.class, () -> store.publishAfterCommit(SECOND, update));
    assertArrayEquals(before, Files.readAllBytes(file()));
    try (java.util.stream.Stream<Path> scratch = Files.list(temp.resolve("table/.hoodie/.temp/embedding-column-info"))) {
      assertEquals(0, scratch.count());
    }
  }

  @Test
  void duplicateNonincreasingSequencesAndRetiredNameReuseInHistoryRefuse() throws Exception {
    add("text", MODEL, FIRST);
    JsonNode original = read();
    for (String corruption : Arrays.asList("duplicate", "decreasing", "reuse")) {
      com.fasterxml.jackson.databind.node.ObjectNode snapshot = original.deepCopy();
      com.fasterxml.jackson.databind.node.ArrayNode events = (com.fasterxml.jackson.databind.node.ArrayNode) snapshot.path("events");
      com.fasterxml.jackson.databind.node.ObjectNode second = events.get(0).deepCopy();
      if (corruption.equals("duplicate")) {
        second.put("sequence", 1);
        second.put("column_name", "another");
      } else if (corruption.equals("decreasing")) {
        ((com.fasterxml.jackson.databind.node.ObjectNode) events.get(0)).put("sequence", 2);
        second.put("sequence", 1);
        second.put("column_name", "another");
      } else {
        second.put("sequence", 2);
        second.put("event", "retire");
        events.add(second.deepCopy());
        second.put("sequence", 3);
        second.put("event", "add");
      }
      events.add(second);
      raw(JSON.writeValueAsString(snapshot));
      assertThrows(IllegalArgumentException.class,
          () -> store().checkAtStart(Collections.emptyMap(), Collections.singletonMap("text", 1536)), corruption);
    }
  }

  @Test
  void unknownHistoricalProvidersRemainOpaqueAndUnknownBirthRetirementRefuses() throws Exception {
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap("legacy", MODEL), Collections.singletonMap("legacy", 1536));
    commit(FIRST);
    store.publishAfterCommit(FIRST, update);
    raw(JSON.writeValueAsString(read()).replace("openai", "historical-provider-no-longer-installed"));
    store().checkAtStart(Collections.emptyMap(), Collections.singletonMap("legacy", 1536));
    IllegalArgumentException conflict = assertThrows(IllegalArgumentException.class,
        () -> store().checkAtStart(Collections.emptyMap(), Collections.singletonMap("legacy", 1536), Collections.singleton("legacy")));
    assertTrue(conflict.getMessage().contains("unknown creation instant"), conflict.getMessage());
  }

  @Test
  void eachNewRoundWarnsAgainAfterMetadataIsLostAgain() throws Exception {
    add("text", MODEL, FIRST);
    Files.delete(file());
    List<LogEvent> warnings = new ArrayList<>();
    AbstractAppender appender = new AbstractAppender("column-info-warnings", null, PatternLayout.createDefaultLayout(), false) {
      @Override
      public void append(LogEvent event) {
        warnings.add(event.toImmutable());
      }
    };
    Logger logger = (Logger) LogManager.getLogger(EmbeddingColumnInfoStore.class);
    appender.start();
    logger.addAppender(appender);
    try {
      EmbeddingColumnInfoStore firstRound = store();
      EmbeddingColumnInfoStore.Update recovery = firstRound.checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536));
      firstRound.checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536));
      commit(SECOND);
      firstRound.publishAfterCommit(SECOND, recovery);
      Files.delete(file());
      EmbeddingColumnInfoStore secondRound = store();
      secondRound.checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536));
      secondRound.checkAtStart(Collections.singletonMap("text", MODEL), Collections.singletonMap("text", 1536));
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
    assertEquals(2, warnings.stream().filter(event -> event.getMessage().getFormattedMessage().contains("VECTOR column text has no model identity")).count());
  }

  @Test
  void tableLockUsesTheEffectiveBuilderProviderRatherThanRawProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), CountingLockProvider.class.getName());
    props.setProperty(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
    String effective = HoodieWriteConfig.newBuilder().withProps(props).withPath(base).forTable("column_info").build().getLockProviderClass();
    assertEquals(InProcessLockProvider.class.getName(), effective, "builder control establishes the effective policy");
    CountingLockProvider.CALLS.set(0);
    new EmbeddingColumnInfoStore(base, conf, props).checkAtStart(Collections.emptyMap(), Collections.emptyMap());
    assertEquals(0, CountingLockProvider.CALLS.get(), "store must honor builder-adjusted provider");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void storeSharesTheWritersExactTrailingSlashLockIdentity(boolean qualified) throws Exception {
    String writerBasePath = qualified ? base + "/" : temp.resolve("table").toString() + "/";
    TypedProperties lockProps = new TypedProperties();
    lockProps.setProperty(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), InProcessLockProvider.class.getName());
    lockProps.setProperty(HoodieLockConfig.LOCK_ACQUIRE_WAIT_TIMEOUT_MS.key(), "100");
    lockProps.setProperty(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.key(), "0");
    HoodieWriteConfig writerConfig = HoodieWriteConfig.newBuilder().withProps(lockProps).withPath(writerBasePath)
        .forTable("column_info").build();
    assertEquals(writerBasePath, writerConfig.getBasePath(), "writer control retains its configured lock identity verbatim");
    EmbeddingColumnInfoStore store = new EmbeddingColumnInfoStore(writerBasePath, conf, lockProps);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    try (LockManager writer = new LockManager(writerConfig, meta.getStorage())) {
      writer.lock();
      try {
        Future<?> checking = pool.submit(() -> {
          started.countDown();
          store.checkAtStart(Collections.emptyMap(), Collections.emptyMap());
        });
        assertTrue(started.await(5, TimeUnit.SECONDS), "store task started while writer holds the actual table lock");
        ExecutionException refused = assertThrows(ExecutionException.class, () -> checking.get(5, TimeUnit.SECONDS),
            "store must not enter while the writer holds the same table's lock");
        assertTrue(refused.getCause() instanceof HoodieLockException, refused.toString());
      } finally {
        writer.unlock();
      }
    } finally {
      pool.shutdownNow();
    }
  }

  public static class CountingLockProvider extends InProcessLockProvider {
    private static final AtomicInteger CALLS = new AtomicInteger();

    public CountingLockProvider(LockConfiguration lockConfiguration, StorageConfiguration<?> storageConf) {
      super(lockConfiguration, storageConf);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      CALLS.incrementAndGet();
      return super.tryLock(time, unit);
    }
  }

  private EmbeddingColumnInfoStore store() {
    return new EmbeddingColumnInfoStore(base, conf, new TypedProperties());
  }

  private void add(String column, EmbeddingColumnInfoStore.Identity identity, String instant) throws Exception {
    EmbeddingColumnInfoStore store = store();
    EmbeddingColumnInfoStore.Update update = store.checkAtStart(Collections.singletonMap(column, identity), Collections.emptyMap());
    commit(instant);
    store.publishAfterCommit(instant, update);
  }

  private void commit(String instant) {
    commit(instant, new HoodieCommitMetadata());
  }

  private void commit(String instant, Map<String, Integer> vectors) {
    List<HoodieSchemaField> fields = new ArrayList<>();
    new java.util.TreeMap<>(vectors).forEach((column, dimension) ->
        fields.add(HoodieSchemaField.of(column, HoodieSchema.createNullable(HoodieSchema.createVector(dimension)))));
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieSchema.createRecord("column_info_record", null, null, fields).toString());
    commit(instant, metadata);
  }

  private void commit(String instant, HoodieCommitMetadata metadata) {
    HoodieInstant inflight = meta.getInstantGenerator().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant);
    meta.reloadActiveTimeline().createNewInstant(inflight);
    meta.getActiveTimeline().saveAsComplete(inflight, Option.of(metadata));
  }

  private void pin(String value) {
    Properties properties = new Properties();
    properties.setProperty(EmbeddingColumnInfoStore.LAYOUT_VERSION.key(), value);
    HoodieTableConfig.update(meta.getStorage(), meta.getMetaPath(), properties);
  }

  private void raw(String json) throws IOException {
    Files.createDirectories(file().getParent());
    Files.write(file(), json.getBytes(StandardCharsets.UTF_8));
  }

  private Path file() {
    return temp.resolve("table/" + EmbeddingColumnInfoStore.RELATIVE_PATH);
  }

  private JsonNode read() throws IOException {
    return JSON.readTree(Files.readAllBytes(file()));
  }

  private HoodieTableMetaClient reload() {
    return HoodieTableMetaClient.builder().setBasePath(base).setConf(conf).build();
  }

  private static EmbeddingColumnInfoStore.Identity identity(String provider, String model, int dimension, String version) {
    return new EmbeddingColumnInfoStore.Identity(provider, model, dimension, version);
  }

  public static class PlainObjectFileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
    @Override
    public java.net.URI getUri() {
      return java.net.URI.create("gs://bucket");
    }
  }

  @Test
  void nonabortableObjectOutputRefusesAtStart() throws Exception {
    org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration(false);
    configuration.set("fs.gs.impl", PlainObjectFileSystem.class.getName());
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    StorageConfiguration<?> objectConf = org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf(configuration);
    String objectBase = "gs://bucket" + temp.resolve("object-table");
    Properties pins = new Properties();
    pins.setProperty(EmbeddingColumnInfoStore.LAYOUT_VERSION.key(), EmbeddingColumnInfoStore.CURRENT_LAYOUT_VERSION);
    HoodieTableMetaClient.newTableBuilder().setTableVersion(9).setTableName("object_column_info")
        .setTableType(HoodieTableType.COPY_ON_WRITE).setAdditionalProperties(pins).initTable(objectConf, objectBase);
    EmbeddingColumnInfoStore objectStore = new EmbeddingColumnInfoStore(objectBase, objectConf, new TypedProperties());
    HoodieIOException failure = assertThrows(HoodieIOException.class,
        () -> objectStore.checkAtStart(Collections.singletonMap("text", MODEL), Collections.emptyMap()));
    assertTrue(failure.getCause().getMessage().contains("fs.capability.outputstream.abortable"), failure.toString());
    assertFalse(Files.exists(temp.resolve("object-table/.hoodie/.aux/embedding-column-info.json")));
  }

}
