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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * The settings a source's job runs with ({@link SourceContract#requiredSettings}), merged into the job's properties by the streamer
 * before anything reads them. A setting the job leaves unset (absent, or blank, under its key and every alternative key) is set;
 * the same value set by the job is accepted; a different value is refused, naming the setting and the source ({@link #builder}).
 * The streamer asks the source's contract for them before it builds the key generator, the write config or the source. A default
 * ({@link Builder#withDefault}) is set when unset and never refused.
 */
public final class RequiredSourceSettings {

  /** No settings: the job's properties are used as they are. */
  public static final RequiredSourceSettings NONE = new RequiredSourceSettings("", Collections.emptyList(), Collections.emptyList());

  /** How a configured value is compared with the required one. */
  private enum Kind {
    /** The same value after trim. */
    EXACT,
    /** The same value after trim, ignoring case (booleans and enum names). */
    IGNORING_CASE,
    /** The same comma-separated fields, each trimmed, in the same order. */
    FIELDS,
    /** Set when unset; any value the job sets is kept. */
    DEFAULT
  }

  private static final class Setting {
    private final ConfigProperty<?> key;
    private final String value;
    private final Kind kind;
    private final ConfigProperty<?> tableKey;
    private final String hint;

    private Setting(ConfigProperty<?> key, String value, Kind kind, ConfigProperty<?> tableKey, String hint) {
      this.key = key;
      this.value = value;
      this.kind = kind;
      this.tableKey = tableKey;
      this.hint = hint;
    }

    private Setting withHint(String hint) {
      return new Setting(key, value, kind, tableKey, hint);
    }

    private boolean accepts(String configured) {
      switch (kind) {
        case EXACT:
          return value.equals(configured.trim());
        case IGNORING_CASE:
          return value.equalsIgnoreCase(configured.trim());
        case FIELDS:
          return value.equals(fields(configured));
        case DEFAULT:
          return true;
        default:
          throw new UnsupportedOperationException("Unknown setting kind " + kind);
      }
    }
  }

  private final String sourceName;
  private final List<Setting> settings;
  private final List<BiConsumer<String, TypedProperties>> checks;

  private RequiredSourceSettings(String sourceName, List<Setting> settings, List<BiConsumer<String, TypedProperties>> checks) {
    this.sourceName = sourceName;
    this.settings = settings;
    this.checks = checks;
  }

  /**
   * A builder of a source's settings; refusals name {@code sourceName}, such as {@code UnstructuredDataSource (mode blob)}.
   */
  public static Builder builder(String sourceName) {
    return new Builder(sourceName);
  }

  /**
   * The job's properties with these settings merged in, then the declared start checks run on them. A required setting the job
   * sets to the same value is written in its required form. {@code props} is never
   * changed: with any setting or check declared the result is a copy, so one job's settings never reach another job built from the
   * same properties; with nothing declared it is {@code props} itself.
   *
   * @throws IllegalArgumentException when the job sets a required setting to another value, or a start check refuses the job
   */
  public TypedProperties apply(TypedProperties props) {
    if (settings.isEmpty() && checks.isEmpty()) {
      return props;
    }
    TypedProperties merged = TypedProperties.copy(props);
    for (Setting setting : settings) {
      String configuredKey = configuredKey(props, setting.key);
      String configured = configuredKey == null ? null : props.get(configuredKey).toString();
      if (configured == null || (setting.kind != Kind.DEFAULT && setting.accepts(configured))) {
        // the job's own equal value is written in the required form: a class name or a field list with spaces is read as is
        // by the writer, which would not find the class or the field
        merged.setProperty(setting.key.key(), setting.value);
      } else if (!setting.accepts(configured)) {
        throw new IllegalArgumentException(sourceName + " requires " + setting.key.key() + "=" + setting.value
            + (setting.kind == Kind.FIELDS ? " (these fields, in this order)" : "")
            + (setting.hint == null ? "" : " (" + setting.hint + ")") + ", configured: "
            + (configuredKey.equals(setting.key.key()) ? "" : configuredKey + "=") + configured);
      }
    }
    for (BiConsumer<String, TypedProperties> check : checks) {
      check.accept(sourceName, merged);
    }
    return merged;
  }

  /**
   * Refuses an existing table whose stored fields differ from a required field list ({@link Builder#requireFields}), such as the
   * record key: the job would write keys of another shape into it.
   *
   * @throws IllegalArgumentException when the table stores other fields, or other fields in another order
   */
  public void checkTable(String basePath, HoodieTableConfig table) {
    for (Setting setting : settings) {
      if (setting.tableKey == null) {
        continue;
      }
      String stored = table.getString(setting.tableKey);
      if (stored == null || !setting.value.equals(fields(stored))) {
        throw new IllegalArgumentException(sourceName + " requires the table's " + setting.tableKey.key() + "=" + setting.value
            + ": the table at " + basePath + " stores " + (stored == null ? "none" : stored) + "; write this job to a new table");
      }
    }
  }

  /**
   * The first of {@code key} and its alternative keys, in that order, that the job sets to a non-blank value; null when none is
   * set.
   */
  private static String configuredKey(TypedProperties props, ConfigProperty<?> key) {
    List<String> keys = new ArrayList<>();
    keys.add(key.key());
    keys.addAll(key.getAlternatives());
    for (String candidate : keys) {
      Object value = props.get(candidate);
      if (value != null && !StringUtils.isNullOrEmpty(value.toString().trim())) {
        return candidate;
      }
    }
    return null;
  }

  /** A comma-separated field list, each field trimmed, empty fields dropped, joined by commas. */
  private static String fields(String list) {
    return Arrays.stream(list.split(",")).map(String::trim).filter(field -> !field.isEmpty()).collect(Collectors.joining(","));
  }

  /** Declares a source's settings. */
  public static final class Builder {
    private final String sourceName;
    private final List<Setting> settings = new ArrayList<>();
    private final List<BiConsumer<String, TypedProperties>> checks = new ArrayList<>();

    private Builder(String sourceName) {
      this.sourceName = sourceName;
    }

    /** {@code key} must be {@code value} (compared after trim); unset, it is set. */
    public Builder require(ConfigProperty<?> key, String value) {
      return add(new Setting(key, value, Kind.EXACT, null, null));
    }

    /** {@code key} must be {@code value} ignoring case (a boolean or an enum name); unset, it is set. */
    public Builder requireIgnoringCase(ConfigProperty<?> key, String value) {
      return add(new Setting(key, value, Kind.IGNORING_CASE, null, null));
    }

    /**
     * {@code key} must list {@code fields} in this order, each compared after trim; unset, it is set. An existing table must store
     * the same fields under {@code tableKey} ({@link #checkTable}).
     */
    public Builder requireFields(ConfigProperty<?> key, List<String> fields, ConfigProperty<?> tableKey) {
      return add(new Setting(key, String.join(",", fields), Kind.FIELDS, tableKey, null));
    }

    /** {@code key} is {@code value} when the job leaves it unset; any value the job sets is kept. */
    public Builder withDefault(ConfigProperty<?> key, String value) {
      return add(new Setting(key, value, Kind.DEFAULT, null, null));
    }

    /** Text the refusal of the last declared setting carries, such as where a job's own value goes instead. */
    public Builder hint(String hint) {
      if (settings.isEmpty()) {
        throw new IllegalStateException("No setting declared to carry the hint " + hint);
      }
      settings.set(settings.size() - 1, settings.get(settings.size() - 1).withHint(hint));
      return this;
    }

    /** A start check run on the merged properties: given the source's name, it refuses a job the source cannot run. */
    public Builder check(BiConsumer<String, TypedProperties> check) {
      checks.add(check);
      return this;
    }

    public RequiredSourceSettings build() {
      return new RequiredSourceSettings(sourceName, Collections.unmodifiableList(new ArrayList<>(settings)),
          Collections.unmodifiableList(new ArrayList<>(checks)));
    }

    private Builder add(Setting setting) {
      for (Setting declared : settings) {
        if (declared.key.key().equals(setting.key.key())) {
          throw new IllegalStateException(sourceName + " declares " + setting.key.key() + " twice");
        }
      }
      settings.add(setting);
      return this;
    }
  }
}
