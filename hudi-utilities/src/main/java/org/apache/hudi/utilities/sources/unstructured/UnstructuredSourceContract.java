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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobSourceSetup;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepLogicVersions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * What the job of {@link UnstructuredDataSource} requires, all of it read from its mode ({@link UnstructuredMode}): the settings
 * it runs with, the mode's transformer in every round with the other mode's refused, the schema guard last in the chain, and the
 * check of the chain and the schemas against the table's contract.
 */
public class UnstructuredSourceContract implements SourceContract {

  /** The key generators of both jobs' tables: the complex one (partitioned) and the non-partitioned one. */
  private static final List<String> KEY_GENERATORS = Arrays.asList(ComplexKeyGenerator.class.getName(),
      NonpartitionedKeyGenerator.class.getName());

  private final UnstructuredMode mode;
  private final UnstructuredTableProperties tablePins;

  /**
   * The contract of the job of {@code props}.
   *
   * @throws IllegalArgumentException when {@code hoodie.unstructured.source.mode} is unset or names no mode
   */
  public UnstructuredSourceContract(TypedProperties props) {
    this.mode = UnstructuredMode.from(props);
    UnstructuredTableProperties.Builder pins = UnstructuredTableProperties.builder(UnstructuredSchemaContract.sourceName(mode))
        .requireTableProperty(UnstructuredTableVersion.BLOB_LAYOUT_VERSION, "1")
        .requireTableProperty(UnstructuredTableVersion.KIND, mode.configValue())
        .requireTableProperty(UnstructuredTableVersion.SCHEMA_VERSION, "1")
        .requireTableProperty(UnstructuredTableVersion.KEY_ENCODING_VERSION, "1");
    if (mode == UnstructuredMode.EMBEDDING) {
      PrepLogicVersions.from(props).requireTableProperties(pins);
    }
    this.tablePins = pins.build();
  }

  /** The job this contract is of. */
  public UnstructuredMode mode() {
    return mode;
  }

  /** Merges this mode's settings once, before any job consumer reads them. */
  @Override
  public TypedProperties prepareJob(TypedProperties jobProps) {
    TypedProperties merged = TypedProperties.copy(jobProps);
    require(merged, HoodieStreamerConfig.START_COMMIT_BEFORE_TRANSFORMER, "true", UnstructuredSourceContract::ignoringCase, "");
    require(merged, KeyGeneratorOptions.RECORDKEY_FIELD_NAME, String.join(",", mode.keyColumns()),
        UnstructuredSourceContract::fields, " (these fields, in this order)");
    if (ConfigUtils.getBooleanWithAltKeys(merged, HoodieWriteConfig.BLOB_MARKERS_ENABLE)) {
      require(merged, HoodieWriteConfig.MARKERS_TYPE, MarkerType.TIMELINE_SERVER_BASED.name(), UnstructuredSourceContract::ignoringCase, "");
      require(merged, HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE, "true", UnstructuredSourceContract::ignoringCase, "");
    }
    require(merged, HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, mode.injectorClass(), String::trim,
        " (a job's own injector goes under " + mode.injectorDelegateKey() + ", and runs first)");
    if (mode == UnstructuredMode.BLOB) {
      setDefault(merged, HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed");
      setDefault(merged, HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED, "false");
    }
    merged = tablePins.apply(merged);
    checkKeyGenerator(UnstructuredSchemaContract.sourceName(mode), merged);
    validateJob(merged);
    return merged;
  }

  @Override
  public Properties tableProperties() {
    return tablePins.tableProperties();
  }

  /** The stream's exact persisted pins, independent of Hudi's generic source preparation. */
  public UnstructuredTableProperties requiredTableProperties() {
    return tablePins;
  }

  /** Checks the table's pins next to the record-key validation, before source construction. */
  public void checkTable(String basePath, HoodieTableConfig table) {
    tablePins.checkTable(basePath, table);
  }

  private void require(TypedProperties props, ConfigProperty<?> key, String value, UnaryOperator<String> normalize, String hint) {
    String configuredKey = configuredKey(props, key);
    String configured = configuredKey == null ? null : props.get(configuredKey).toString();
    if (configured != null && !normalize.apply(value).equals(normalize.apply(configured))) {
      throw new IllegalArgumentException(UnstructuredSchemaContract.sourceName(mode) + " requires " + key.key() + "=" + value + hint
          + ", configured: " + (configuredKey.equals(key.key()) ? "" : configuredKey + "=") + configured);
    }
    props.setProperty(key.key(), value);
  }

  private static void setDefault(TypedProperties props, ConfigProperty<?> key, String value) {
    if (configuredKey(props, key) == null) {
      props.setProperty(key.key(), value);
    }
  }

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

  private static String ignoringCase(String value) {
    return value.trim().toLowerCase(Locale.ROOT);
  }

  /** A record key is a field list with significant order, independent of surrounding whitespace. */
  public static String fields(String value) {
    return Arrays.stream(value.split(",")).map(String::trim).filter(field -> !field.isEmpty()).collect(Collectors.joining(","));
  }

  @Override
  public List<String> transformerChain(List<String> configured) {
    return UnstructuredTransformerChain.compose(mode, configured);
  }

  /**
   * Reads the job and the cloud, refuses a key of the other cloud, and validates the job's other properties
   * ({@link BlobSourceSetup#validate}), the table-version floor among them; returns {@code props} unchanged. The streamer runs it
   * with the merge, before it builds anything or creates the table, and the source again as it is built: one copy of the checks.
   *
   * @throws IllegalArgumentException          naming the setting and {@code UnstructuredDataSource (mode ...)}
   * @throws org.apache.hudi.exception.HoodieValidationException when the job's table version is below the tables' floor (9)
   */
  public static TypedProperties validateJob(TypedProperties props) {
    UnstructuredMode mode = UnstructuredMode.from(props);
    UnstructuredCloud cloud = UnstructuredCloud.from(props);
    String name = UnstructuredSchemaContract.sourceName(mode);
    cloud.validate(props, name);
    return BlobSourceSetup.validate(props, mode, name);
  }

  /**
   * Refuses a key generator, named or inferred from the record key and partition fields, other than {@link #KEY_GENERATORS}; a name
   * with spaces is also refused, since the writer loads it as written.
   */
  private static void checkKeyGenerator(String sourceName, TypedProperties props) {
    String keyGenerator = HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName(props);
    if (!KEY_GENERATORS.contains(keyGenerator)) {
      throw new IllegalArgumentException(sourceName + " supports only " + String.join(" or ", KEY_GENERATORS) + ", configured: " + keyGenerator);
    }
  }

}
