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

import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.streamer.CompletedWrite;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stream-owned model history in one auxiliary JSON file. Start checks are read-only; changes become durable only after a
 * completed table commit. Each operation holds the configured table lock, and publication rereads history under that lock.
 */
public class EmbeddingColumnInfoStore {
  /** Governs the parser of the auxiliary history file, independently of blob layout and table schema versions. */
  public static final ConfigProperty<String> LAYOUT_VERSION = ConfigProperty
      .key("hoodie.table.embedding.column.info.layout.version").noDefaultValue();
  public static final String RELATIVE_PATH = ".hoodie/.aux/embedding-column-info.json";
  static final String FENCE_PATH = ".hoodie/.aux/embedding-column-fence.json";
  private static final String FENCE_SCRATCH = ".hoodie/.temp/embedding-column-fence";
  private static final String SCRATCH_DIRECTORY = ".hoodie/.temp/embedding-column-info";
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingColumnInfoStore.class);
  private static final ObjectMapper JSON = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final String FIELD_REVISION = "revision";
  private static final String FIELD_INSTANT = "instant";
  private static final String FIELD_LAYOUT_VERSION = "layout_version";
  private static final String EVENT_ADD = "add";
  private static final String EVENT_RETIRE = "retire";
  private static final String EVENT_RECOVER = "recover";
  private static final String FIELD_DIMENSION = "dimension";
  private static final String FIELD_EVENTS = "events";
  private static final String FIELD_COLUMN_NAME = "column_name";
  private static final String FIELD_EVENT = "event";
  private static final String FIELD_IDENTITY = "identity";
  private static final String FIELD_SEQUENCE = "sequence";
  private static final String FIELD_PROVIDER = "provider";
  private static final String FIELD_MODEL_NAME = "model_name";
  private static final String FIELD_MODEL_VERSION = "model_version";
  private static final String FIELD_ADDED_AT_INSTANT = "added_at_instant";
  private static final String VECTOR_COLUMN = "VECTOR column ";
  private static final Set<String> SNAPSHOT_FIELDS = fields(FIELD_LAYOUT_VERSION, FIELD_REVISION, FIELD_EVENTS);
  private static final Set<String> EVENT_FIELDS = fields(FIELD_SEQUENCE, FIELD_COLUMN_NAME, FIELD_EVENT, FIELD_INSTANT, FIELD_ADDED_AT_INSTANT, FIELD_IDENTITY);
  private static final Set<String> IDENTITY_FIELDS = fields(FIELD_PROVIDER, FIELD_MODEL_NAME, FIELD_DIMENSION, FIELD_MODEL_VERSION);

  private final String basePath;
  // The table writer passes its configured path verbatim; lock providers may key on that exact string.
  private final String writerLockBasePath;
  private final StorageConfiguration<?> storageConf;
  private final TypedProperties props;
  private final HoodieStorage storage;
  private final HoodieStorage auxiliaryStorage;
  private final StoragePath path;
  private final EmbeddingColumnInfoAtomicSwap swap;
  private final Set<String> warned = ConcurrentHashMap.newKeySet();

  /** Uses the table's configured lock provider and a backend with complete-file atomic replacement. */
  public EmbeddingColumnInfoStore(String basePath, StorageConfiguration<?> storageConf, TypedProperties props) {
    this(basePath, storageConf, props, new EmbeddingColumnInfoAtomicSwap());
  }

  EmbeddingColumnInfoStore(String basePath, StorageConfiguration<?> storageConf, TypedProperties props,
                           EmbeddingColumnInfoAtomicSwap swap) {
    this.basePath = new StoragePath(basePath).toString();
    this.writerLockBasePath = basePath;
    this.storageConf = storageConf.newInstance();
    this.props = TypedProperties.copy(props);
    this.storage = HoodieStorageUtils.getStorage(new StoragePath(this.basePath), this.storageConf);
    this.auxiliaryStorage = EmbeddingColumnInfoAtomicSwap.metadataStorage(storage);
    this.path = new StoragePath(this.basePath, RELATIVE_PATH);
    this.swap = Objects.requireNonNull(swap, "swap");
  }

  /** Checks configured model columns, without requesting logical retirements. */
  public Update checkAtStart(Map<String, Identity> configured, Map<String, Integer> existingVectorColumns) {
    return checkAtStart(configured, existingVectorColumns, Collections.emptySet());
  }

  /**
   * Refuses present mismatches, retired-name reuse and invalid retirements before prep. A missing identity warns once per
   * table/column and skips only the identity check; the physical VECTOR dimension is still checked. Inactive columns remain
   * active unless explicitly retired. Retirement is logical and need not remove a physical schema field. Repeating a
   * retirement request is a no-op; configuring a model under that retired name remains forbidden. When an earlier committed
   * schema transition proves a missing entry's creation instant, the returned in-memory Update retains it for post-commit ADD
   * publication. This start check never writes metadata.
   */
  public Update checkAtStart(Map<String, Identity> configured, Map<String, Integer> existingVectorColumns,
                             Set<String> retiredColumns) {
    Update update = new Update(basePath, configured, existingVectorColumns, retiredColumns);
    return locked(meta -> {
      Snapshot snapshot = read(meta);
      validate(readFence(meta, snapshot), update, false);
      for (String column : update.configured.keySet()) {
        if (update.existing.containsKey(column) && !snapshot.latest.containsKey(column) && warned.add(column)) {
          LOG.warn("Embedding VECTOR column {} has no model identity information in auxiliary history; recovering after a successful commit", column);
        }
      }
      return update.withKnownBirths(committedColumnBirths(meta, snapshot, update));
    });
  }

  /**
   * After a successful table commit, appends new/recovered identities and requested retirements to the latest history.
   * Repeating an already-applied update is a no-op. Pre-publication failures preserve the previous bytes. Object-store
   * close acknowledgement failures may occur after the complete new object is durable; visibility remains old or new.
   */
  public void publishAfterCommit(String instant, Update update) {
    requireInstant(instant, FIELD_INSTANT);
    if (!basePath.equals(update.basePath)) {
      throw invalid("Update belongs to another table");
    }
    locked(meta -> {
      if (!CompletedWrite.exists(meta, instant)) {
        throw invalid("Cannot publish embedding column information before completed table instant " + instant);
      }
      Snapshot published = read(meta);
      Snapshot snapshot = readFence(meta, published);
      validate(snapshot, update, false);
      Snapshot updated = append(snapshot, update, instant);
      // An empty in-memory snapshot has no persisted layout-1 form.
      if (!updated.events.isEmpty() && auxiliaryStorage.exists(new StoragePath(basePath, FENCE_PATH))) {
        // A manual publisher is already past its data commit; retain its confirmed state in the same authority.
        writeFence(updated, updated, instant);
      }
      if (!Arrays.equals(serialize(updated), serialize(published))) {
        write(meta, updated);
      }
      return null;
    });
  }

  private Snapshot append(Snapshot snapshot, Update update, String instant) {
    List<Event> appended = new ArrayList<>(snapshot.events);
    long sequence = snapshot.events.isEmpty() ? 0 : snapshot.events.get(snapshot.events.size() - 1).sequence;
    for (String column : update.retired) {
      Event previous = snapshot.latest.get(column);
      if (!EVENT_RETIRE.equals(previous.action)) {
        appended.add(new Event(next(sequence), column, EVENT_RETIRE, instant, previous.addedAt, previous.identity));
        sequence++;
      }
    }
    for (Map.Entry<String, Identity> entry : update.configured.entrySet()) {
      if (!snapshot.latest.containsKey(entry.getKey())) {
        boolean existing = update.existing.containsKey(entry.getKey());
        // A surviving snapshot plus an exact schema transition can prove an earlier failed publication's birth.
        // Whole-file loss retains the recovery contract even when this Update once observed a surviving snapshot.
        String knownBirth = snapshot.revision == 0 ? null : update.knownBirths.get(entry.getKey());
        boolean recovery = existing && knownBirth == null;
        String eventInstant = knownBirth == null ? instant : knownBirth;
        appended.add(new Event(next(sequence), entry.getKey(), recovery ? EVENT_RECOVER : EVENT_ADD, eventInstant,
            recovery ? null : eventInstant, entry.getValue()));
        sequence++;
      }
    }
    if (appended.size() == snapshot.events.size()) {
      return snapshot;
    }
    return new Snapshot(next(snapshot.revision), appended);
  }

  /** Called by the driver's commit callback while the data-table transaction lock is held. Never acquires another lock. */
  public void validateBeforeCommit(HoodieTableMetaClient committingTable, String instant, Update update) {
    requireInstant(instant, "instant");
    if (!basePath.equals(committingTable.getBasePath().toString()) || !basePath.equals(update.basePath)) {
      throw invalid("Embedding identity validation belongs to another table");
    }
    try {
      HoodieTableMetaClient fresh = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf.newInstance()).build();
      Snapshot published = read(fresh);
      Snapshot committed = readFence(fresh, published);
      validate(committed, update, false);
      Snapshot candidate = append(committed, update, instant);
      if (candidate.events.isEmpty()) {
        return;
      }
      writeFence(committed, candidate, instant);
    } catch (IOException e) {
      throw new HoodieIOException("Cannot fence embedding model identity before commit " + instant, e);
    }
  }

  private void writeFence(Snapshot committed, Snapshot candidate, String instant) throws IOException {
    ObjectNode envelope = JSON.createObjectNode();
    envelope.put("layout_version", 1);
    if (committed.events.isEmpty()) {
      envelope.putNull("committed");
    } else {
      envelope.set("committed", JSON.readTree(serialize(committed)));
    }
    ObjectNode pending = envelope.putObject("pending");
    pending.put("instant", instant);
    pending.set("snapshot", JSON.readTree(serialize(candidate)));
    replaceComplete(new StoragePath(basePath, FENCE_PATH), new StoragePath(basePath, FENCE_SCRATCH),
        JSON.writeValueAsBytes(envelope));
  }

  /** Publishes the latest committed fence, including updates whose earlier callbacks were delayed. */
  void publishCommittedState(String instant, Update update) {
    locked(meta -> {
      if (!CompletedWrite.exists(meta, instant)) {
        throw invalid("Cannot publish embedding column information before completed table instant " + instant);
      }
      Snapshot published = read(meta);
      Snapshot committed = readFence(meta, published);
      if (!committed.events.isEmpty() && !Arrays.equals(serialize(published), serialize(committed))) {
        write(meta, committed);
      }
      return null;
    });
  }

  private Snapshot readFence(HoodieTableMetaClient meta, Snapshot fallback) throws IOException {
    StoragePath fence = new StoragePath(basePath, FENCE_PATH);
    EmbeddingColumnInfoAtomicSwap.checkLocalChecksum(auxiliaryStorage, fence);
    if (!auxiliaryStorage.exists(fence)) {
      return fallback;
    }
    JsonNode root;
    try (InputStream input = auxiliaryStorage.open(fence)) {
      root = JSON.readTree(input);
    }
    exactFields(root, fields("layout_version", "committed", "pending"), fields("layout_version", "committed", "pending"), "identity fence");
    if (positiveLong(root.get("layout_version"), "identity fence layout_version") != 1) {
      throw invalid("Unsupported embedding identity fence layout version");
    }
    Snapshot committed = root.get("committed").isNull() ? fallback : parseSnapshot(root.get("committed"));
    JsonNode pending = root.get("pending");
    exactFields(pending, fields("instant", "snapshot"), fields("instant", "snapshot"), "pending identity");
    String instant = text(pending.get("instant"), "pending instant");
    requireInstant(instant, "pending instant");
    Snapshot candidate = parseSnapshot(pending.get("snapshot"));
    return mergePublished(CompletedWrite.exists(meta, instant) ? candidate : committed, fallback);
  }

  private Snapshot mergePublished(Snapshot authority, Snapshot published) throws IOException {
    List<Event> merged = new ArrayList<>(authority.events);
    long sequence = merged.isEmpty() ? 0 : merged.get(merged.size() - 1).sequence;
    Set<String> missing = new HashSet<>(published.latest.keySet());
    missing.removeAll(authority.latest.keySet());
    for (Event event : published.events) {
      if (missing.contains(event.column)) {
        merged.add(new Event(next(sequence), event.column, event.action, event.instant, event.addedAt, event.identity));
        sequence++;
      }
    }
    for (Map.Entry<String, Event> entry : published.latest.entrySet()) {
      Event previous = authority.latest.get(entry.getKey());
      Event event = entry.getValue();
      if (previous == null) {
        continue;
      }
      if (!previous.identity.equals(event.identity)) {
        throw invalid("VECTOR column " + event.column + " has conflicting committed model identities");
      }
      if (EVENT_RETIRE.equals(event.action) && !EVENT_RETIRE.equals(previous.action)) {
        merged.add(new Event(next(sequence), event.column, EVENT_RETIRE, event.instant, previous.addedAt, previous.identity));
        sequence++;
      }
    }
    // A delayed callback may merge the same older fence again. Keep the complete published state when nothing changed.
    if (published.revision >= authority.revision
        && Arrays.equals(serialize(new Snapshot(published.revision, merged)), serialize(published))) {
      return published;
    }
    if (merged.size() == authority.events.size() && authority.revision >= published.revision) {
      return authority;
    }
    return new Snapshot(next(Math.max(authority.revision, published.revision)), merged);
  }

  private void replaceComplete(StoragePath target, StoragePath directory, byte[] bytes) throws IOException {
    auxiliaryStorage.createDirectory(directory);
    auxiliaryStorage.createDirectory(target.getParent());
    StoragePath scratch = new StoragePath(directory, UUID.randomUUID() + ".json");
    try {
      try (OutputStream output = auxiliaryStorage.create(scratch, false)) {
        output.write(bytes);
      }
      swap.replace(auxiliaryStorage, scratch, target);
    } finally {
      auxiliaryStorage.deleteFile(scratch);
    }
  }

  /**
   * Finds actual creation instants only from exact committed writer-schema transitions, never from schema-resolver
   * fallbacks or the first visible containing schema. A schema-less operation that may change schema breaks the absence
   * proof. A timestamp/action-only archive query rejects gaps between retained schemas, including after savepoint removal.
   * Archived/unprovable history and whole-file loss therefore keep the recover/unknown-birth policy.
   */
  private Map<String, String> committedColumnBirths(HoodieTableMetaClient meta, Snapshot snapshot, Update update) throws IOException {
    if (snapshot.revision == 0) {
      return Collections.emptyMap();
    }
    Map<String, Integer> missing = missingIdentityColumns(snapshot, update);
    if (missing.isEmpty()) {
      return Collections.emptyMap();
    }
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    List<HoodieInstant> instants = new ArrayList<>(timeline.getInstants());
    instants.sort(Comparator.comparing(HoodieInstant::requestedTime));
    Map<String, String> knownAbsent = new HashMap<>();
    Set<String> seen = new HashSet<>();
    Map<String, String> births = new TreeMap<>();
    for (HoodieInstant instant : instants) {
      HoodieCommitMetadata commit = timeline.readCommitMetadata(instant);
      String schemaJson = commit.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
      if (schemaJson == null || schemaJson.trim().isEmpty()) {
        if (WriteOperationType.canUpdateSchema(commit.getOperationType())) {
          knownAbsent.clear();
        }
      } else {
        recordSchemaTransitions(meta, instant, HoodieSchema.parse(schemaJson), missing, knownAbsent, seen, births);
        if (seen.size() == missing.size()) {
          break;
        }
      }
    }
    return births;
  }

  /** Returns the configured, physically present columns with no history entry, mapped to their configured dimension. */
  private static Map<String, Integer> missingIdentityColumns(Snapshot snapshot, Update update) {
    Map<String, Integer> missing = new TreeMap<>();
    update.configured.forEach((column, identity) -> {
      if (update.existing.containsKey(column) && !snapshot.latest.containsKey(column)) {
        missing.put(column, identity.dimension);
      }
    });
    return missing;
  }

  /**
   * Records, for each not-yet-seen missing column, its absence from or first appearance in one committed schema; a first
   * appearance right after a proven absence, with the configured VECTOR dimension and no archived write between, is a birth.
   */
  private void recordSchemaTransitions(HoodieTableMetaClient meta, HoodieInstant instant, HoodieSchema schema,
                                              Map<String, Integer> missing, Map<String, String> knownAbsent,
                                              Set<String> seen, Map<String, String> births) {
    for (Map.Entry<String, Integer> entry : missing.entrySet()) {
      String column = entry.getKey();
      if (seen.contains(column)) {
        continue;
      }
      Option<HoodieSchemaField> field = schema.getField(column);
      if (!field.isPresent()) {
        knownAbsent.put(column, instant.requestedTime());
      } else {
        seen.add(column);
        HoodieSchema type = field.get().schema().getNonNullType();
        String absentAt = knownAbsent.remove(column);
        if (absentAt != null && type.getType() == HoodieSchemaType.VECTOR
            && ((HoodieSchema.Vector) type).getDimension() == entry.getValue()
            && !hasArchivedWriteBetween(meta, absentAt, instant.requestedTime())) {
          births.put(column, instant.requestedTime());
        }
      }
    }
  }

  /** Checks only archive timestamps/actions, because retained savepoints can leave holes in the active write timeline. */
  private boolean hasArchivedWriteBetween(HoodieTableMetaClient meta, String absentAt, String presentAt) {
    ArchivedTimelineLoader loader = meta.getTableFormat().getTimelineFactory().createArchivedTimelineLoader();
    HoodieArchivedTimeline.TimeRangeFilter interval = new HoodieArchivedTimeline.TimeRangeFilter(absentAt, presentAt) {
      @Override
      public boolean isInRange(String instant) {
        return super.isInRange(instant) && !instant.equals(presentAt);
      }
    };
    AtomicBoolean found = new AtomicBoolean();
    loader.loadInstants(meta, interval, HoodieArchivedTimeline.LoadMode.ACTION, archived -> {
      Object value = archived.get("action");
      if (value == null) {
        // A historical record without an action cannot establish that the interval has no archived writes.
        return true;
      }
      String action = value.toString();
      return HoodieTimeline.COMMIT_ACTION.equals(action) || HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)
          || HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action);
    }, (instant, archived) -> found.set(true), Option.of(1));
    return found.get();
  }

  private void validate(Snapshot snapshot, Update update, boolean atStart) {
    for (Map.Entry<String, Identity> entry : update.configured.entrySet()) {
      validateConfigured(snapshot, entry.getKey(), entry.getValue(), update.existing.get(entry.getKey()), atStart);
    }
    for (String column : update.retired) {
      validateRetired(snapshot, column);
    }
  }

  /** A configured column: its physical dimension and its identity history agree with the configuration. */
  private void validateConfigured(Snapshot snapshot, String column, Identity configured, Integer physicalDimension, boolean atStart) {
    if (physicalDimension != null && physicalDimension != configured.dimension) {
      throw invalid(VECTOR_COLUMN + column + " has dimension " + physicalDimension
          + ", configured model requires " + configured.dimension);
    }
    Event current = snapshot.latest.get(column);
    if (current != null) {
      if (EVENT_RETIRE.equals(current.action)) {
        throw invalid(VECTOR_COLUMN + column + " is retired; use a new physical column name");
      }
      if (!current.identity.equals(configured)) {
        throw invalid(VECTOR_COLUMN + column + " has a different model identity; use a new physical column name");
      }
    } else if (atStart && physicalDimension != null && warned.add(column)) {
      LOG.warn("Embedding VECTOR column {} has no model identity information; skipping identity validation and recovering "
          + "the configured identity after a successful commit (dimension validation still applies)", column);
    }
  }

  /** A column to retire has a known, non-recovered identity history. */
  private static void validateRetired(Snapshot snapshot, String column) {
    Event current = snapshot.latest.get(column);
    if (current == null) {
      throw invalid("Cannot retire " + VECTOR_COLUMN + column + " without known active identity history");
    }
    if (current.addedAt == null) {
      throw invalid("Cannot retire recovered " + VECTOR_COLUMN + column
          + " with unknown creation instant: layout 1 permits null added_at_instant only on recover events");
    }
  }

  private Snapshot read(HoodieTableMetaClient meta) throws IOException {
    String version = meta.getTableConfig().getString(LAYOUT_VERSION);
    if (version != null && !"1".equals(version)) {
      throw invalid("Unsupported " + LAYOUT_VERSION.key() + "=" + version + "; supported layout version is 1");
    }
    boolean exists = auxiliaryStorage.exists(path);
    if (version == null && exists) {
      throw invalid("Embedding column info file exists without governing " + LAYOUT_VERSION.key());
    }
    EmbeddingColumnInfoAtomicSwap.checkLocalChecksum(auxiliaryStorage, path);
    if (!exists) {
      return new Snapshot(0, Collections.emptyList());
    }
    JsonNode root;
    try (InputStream input = auxiliaryStorage.open(path)) {
      root = JSON.readTree(input);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalArgumentException("Malformed embedding column info JSON at " + path, e);
    }
    return parseSnapshot(root);
  }

  private static Snapshot parseSnapshot(JsonNode root) {
    exactFields(root, SNAPSHOT_FIELDS, SNAPSHOT_FIELDS, "snapshot");
    if (positiveLong(root.get(FIELD_LAYOUT_VERSION), FIELD_LAYOUT_VERSION) != 1) {
      throw invalid("Embedding column info layout_version disagrees with table property 1");
    }
    long revision = positiveLong(root.get(FIELD_REVISION), FIELD_REVISION);
    JsonNode events = root.get(FIELD_EVENTS);
    if (!events.isArray()) {
      throw invalid("events must be an array");
    }
    List<Event> parsed = new ArrayList<>();
    Map<String, Event> latest = new HashMap<>();
    long sequence = 0;
    for (JsonNode node : events) {
      Event event = parseEvent(node);
      if (event.sequence <= sequence) {
        throw invalid("Event sequences must be unique and strictly increasing");
      }
      checkFollows(latest.get(event.column), event);
      latest.put(event.column, event);
      parsed.add(event);
      sequence = event.sequence;
    }
    return new Snapshot(revision, parsed);
  }

  /** A retire event follows a live event of the same identity and creation instant; any other event is the column's first. */
  private static void checkFollows(Event previous, Event event) {
    if (EVENT_RETIRE.equals(event.action)) {
      if (previous == null || EVENT_RETIRE.equals(previous.action) || !previous.identity.equals(event.identity)
          || !Objects.equals(previous.addedAt, event.addedAt)) {
        throw invalid("Invalid retirement history for VECTOR column " + event.column);
      }
    } else if (previous != null) {
      throw invalid(VECTOR_COLUMN + event.column + " has repeated creation or reuses a retired name");
    }
  }

  private static Event parseEvent(JsonNode node) {
    exactFields(node, EVENT_FIELDS, EVENT_FIELDS, FIELD_EVENT);
    long sequence = positiveLong(node.get(FIELD_SEQUENCE), FIELD_SEQUENCE);
    String column = text(node.get(FIELD_COLUMN_NAME), FIELD_COLUMN_NAME);
    String action = text(node.get(FIELD_EVENT), FIELD_EVENT);
    if (!Arrays.asList(EVENT_ADD, EVENT_RETIRE, EVENT_RECOVER).contains(action)) {
      throw invalid("Unknown column event " + action);
    }
    String instant = text(node.get(FIELD_INSTANT), FIELD_INSTANT);
    requireInstant(instant, FIELD_INSTANT);
    JsonNode birth = node.get(FIELD_ADDED_AT_INSTANT);
    String addedAt = birth.isNull() ? null : text(birth, FIELD_ADDED_AT_INSTANT);
    if (addedAt == null && !EVENT_RECOVER.equals(action)) {
      throw invalid("Null added_at_instant is permitted only for recover events");
    }
    if (addedAt != null) {
      requireInstant(addedAt, FIELD_ADDED_AT_INSTANT);
    }
    if (EVENT_ADD.equals(action) && !instant.equals(addedAt)) {
      throw invalid("An add event must retain its actual creation instant");
    }
    JsonNode identity = node.get(FIELD_IDENTITY);
    exactFields(identity, IDENTITY_FIELDS, fields(FIELD_PROVIDER, FIELD_MODEL_NAME, FIELD_DIMENSION), FIELD_IDENTITY);
    long dimension = positiveLong(identity.get(FIELD_DIMENSION), FIELD_DIMENSION);
    if (dimension > Integer.MAX_VALUE) {
      throw invalid("dimension exceeds integer range");
    }
    Identity model = new Identity(text(identity.get(FIELD_PROVIDER), FIELD_PROVIDER), text(identity.get(FIELD_MODEL_NAME), FIELD_MODEL_NAME),
        (int) dimension, identity.has(FIELD_MODEL_VERSION) ? text(identity.get(FIELD_MODEL_VERSION), FIELD_MODEL_VERSION) : null);
    return new Event(sequence, column, action, instant, addedAt, model);
  }

  private void write(HoodieTableMetaClient meta, Snapshot snapshot) throws IOException {
    StoragePath directory = new StoragePath(basePath, SCRATCH_DIRECTORY);
    auxiliaryStorage.createDirectory(directory);
    auxiliaryStorage.createDirectory(path.getParent());
    StoragePath scratch = new StoragePath(directory, UUID.randomUUID() + ".json");
    try {
      byte[] bytes = serialize(snapshot);
      try (OutputStream output = auxiliaryStorage.create(scratch, false)) {
        output.write(bytes);
      }
      // Establish the parser selector first: interruption leaves a supported missing file, recovered by the next round.
      if (meta.getTableConfig().getString(LAYOUT_VERSION) == null) {
        Properties properties = new Properties();
        properties.setProperty(LAYOUT_VERSION.key(), "1");
        HoodieTableConfig.update(storage, meta.getMetaPath(), properties);
      }
      swap.replace(auxiliaryStorage, scratch, path);
    } finally {
      try {
        auxiliaryStorage.deleteFile(scratch);
      } catch (IOException cleanupFailure) {
        LOG.warn("Cannot remove owned embedding column info scratch {}", scratch, cleanupFailure);
      }
    }
  }

  private static byte[] serialize(Snapshot snapshot) throws IOException {
    ObjectNode root = JSON.createObjectNode();
    root.put(FIELD_LAYOUT_VERSION, 1);
    root.put(FIELD_REVISION, snapshot.revision);
    ArrayNode events = root.putArray(FIELD_EVENTS);
    for (Event event : snapshot.events) {
      ObjectNode node = events.addObject();
      node.put(FIELD_SEQUENCE, event.sequence);
      node.put(FIELD_COLUMN_NAME, event.column);
      node.put(FIELD_EVENT, event.action);
      node.put(FIELD_INSTANT, event.instant);
      if (event.addedAt == null) {
        node.putNull(FIELD_ADDED_AT_INSTANT);
      } else {
        node.put(FIELD_ADDED_AT_INSTANT, event.addedAt);
      }
      ObjectNode identity = node.putObject(FIELD_IDENTITY);
      identity.put(FIELD_PROVIDER, event.identity.provider);
      identity.put(FIELD_MODEL_NAME, event.identity.modelName);
      identity.put(FIELD_DIMENSION, event.identity.dimension);
      if (event.identity.modelVersion != null) {
        identity.put(FIELD_MODEL_VERSION, event.identity.modelVersion);
      }
    }
    return JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root);
  }

  private <T> T locked(Operation<T> operation) {
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf.newInstance()).build();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProps(props).withPath(writerLockBasePath)
        .forTable(meta.getTableConfig().getTableName()).build();
    if (config.getLockProviderClass() == null) {
      if (config.getWriteConcurrencyMode() != WriteConcurrencyMode.SINGLE_WRITER) {
        throw invalid("Embedding column information requires a configured table lock provider for " + config.getWriteConcurrencyMode());
      }
      config.setValue(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME, InProcessLockProvider.class.getName());
    }
    try (LockManager manager = new LockManager(config, storage)) {
      manager.lock();
      try {
        // Table properties may have changed while this operation waited for the lock.
        return operation.run(HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf.newInstance()).build());
      } finally {
        manager.unlock();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Cannot read or publish embedding column information at " + path, e);
    }
  }

  private interface Operation<T> {
    T run(HoodieTableMetaClient meta) throws IOException;
  }

  private static Set<String> fields(String... names) {
    return new HashSet<>(Arrays.asList(names));
  }

  private static void exactFields(JsonNode node, Set<String> allowed, Set<String> required, String label) {
    if (node == null || !node.isObject()) {
      throw invalid(label + " must be an object");
    }
    Iterator<String> keys = node.fieldNames();
    while (keys.hasNext()) {
      String key = keys.next();
      if (!allowed.contains(key)) {
        throw invalid("Unknown " + label + " field " + key);
      }
    }
    for (String key : required) {
      if (!node.has(key)) {
        throw invalid("Missing " + label + " field " + key);
      }
    }
  }

  private static long positiveLong(JsonNode node, String label) {
    if (node == null || !node.isIntegralNumber() || !node.canConvertToLong() || node.longValue() < 1) {
      throw invalid(label + " must be a positive integer");
    }
    return node.longValue();
  }

  private static String text(JsonNode node, String label) {
    if (node == null || !node.isTextual()) {
      throw invalid(label + " must be a nonempty string");
    }
    return nonempty(node.textValue(), label);
  }

  private static String nonempty(String value, String label) {
    if (value == null || value.trim().isEmpty() || !value.equals(value.trim())) {
      throw invalid(label + " must be a nonempty canonical string without surrounding whitespace");
    }
    return value;
  }

  private static void requireInstant(String instant, String label) {
    nonempty(instant, label);
    if (!instant.matches("\\d+")) {
      throw invalid(label + " must be a Hudi requested-instant string");
    }
  }

  private static long next(long value) {
    if (value == Long.MAX_VALUE) {
      throw invalid("Embedding column info counter exhausted");
    }
    return value + 1;
  }

  private static IllegalArgumentException invalid(String message) {
    return new IllegalArgumentException(message);
  }

  /** In-memory intent only; never serialized into the table schema or a pending sidecar. */
  public static final class Update implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String basePath;
    private final Map<String, Identity> configured;
    private final Map<String, Integer> existing;
    private final Set<String> retired;
    private final Map<String, String> knownBirths;

    private Update(String basePath, Map<String, Identity> configured, Map<String, Integer> existing, Set<String> retired) {
      this(basePath, configured, existing, retired, Collections.emptyMap());
    }

    private Update(String basePath, Map<String, Identity> configured, Map<String, Integer> existing, Set<String> retired,
                   Map<String, String> knownBirths) {
      this.basePath = basePath;
      this.knownBirths = Collections.unmodifiableMap(new TreeMap<>(knownBirths));
      this.configured = Collections.unmodifiableMap(new TreeMap<>(configured));
      this.existing = Collections.unmodifiableMap(new TreeMap<>(existing));
      this.retired = Collections.unmodifiableSet(new TreeSet<>(retired));
      for (Map.Entry<String, Identity> entry : this.configured.entrySet()) {
        nonempty(entry.getKey(), FIELD_COLUMN_NAME);
        Objects.requireNonNull(entry.getValue(), "model identity");
        if (this.retired.contains(entry.getKey())) {
          throw invalid(VECTOR_COLUMN + entry.getKey() + " cannot be configured and retired in the same round");
        }
      }
      this.retired.forEach(column -> nonempty(column, FIELD_COLUMN_NAME));
      this.existing.forEach((column, dimension) -> {
        nonempty(column, FIELD_COLUMN_NAME);
        if (dimension == null || dimension < 1) {
          throw invalid("VECTOR dimension must be positive for column " + column);
        }
      });
    }

    private Update withKnownBirths(Map<String, String> births) {
      return new Update(basePath, configured, existing, retired, births);
    }
  }

  /** The only allow-listed model fields accepted by the history writer; no connection/configuration map is retained. */
  public static final class Identity implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String provider;
    private final String modelName;
    private final int dimension;
    private final String modelVersion;

    public Identity(String provider, String modelName, int dimension, String modelVersion) {
      this.provider = nonempty(provider, FIELD_PROVIDER);
      this.modelName = nonempty(modelName, FIELD_MODEL_NAME);
      if (dimension < 1) {
        throw invalid("dimension must be positive");
      }
      this.dimension = dimension;
      this.modelVersion = modelVersion == null ? null : nonempty(modelVersion, FIELD_MODEL_VERSION);
    }

    public String getProvider() {
      return provider;
    }

    public String getModelName() {
      return modelName;
    }

    public int getDimension() {
      return dimension;
    }

    public String getModelVersion() {
      return modelVersion;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Identity)) {
        return false;
      }
      Identity that = (Identity) other;
      return dimension == that.dimension && provider.equals(that.provider) && modelName.equals(that.modelName)
          && Objects.equals(modelVersion, that.modelVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(provider, modelName, dimension, modelVersion);
    }
  }

  private static final class Event {
    private final long sequence;
    private final String column;
    private final String action;
    private final String instant;
    private final String addedAt;
    private final Identity identity;

    private Event(long sequence, String column, String event, String instant, String addedAt, Identity identity) {
      this.sequence = sequence;
      this.column = column;
      this.action = event;
      this.instant = instant;
      this.addedAt = addedAt;
      this.identity = identity;
    }
  }

  private static final class Snapshot {
    private final long revision;
    private final List<Event> events;
    private final Map<String, Event> latest = new LinkedHashMap<>();

    private Snapshot(long revision, List<Event> events) {
      this.revision = revision;
      this.events = events;
      events.forEach(event -> latest.put(event.column, event));
    }
  }
}
