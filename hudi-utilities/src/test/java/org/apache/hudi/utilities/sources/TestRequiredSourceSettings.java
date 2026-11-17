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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RequiredSourceSettings}: a source contract's declaration ({@link SourceContract#requiredSettings}), the merge into a job's
 * properties and its refusals, and the existing-table check. The unstructured data source's own settings are in
 * {@code TestUnstructuredSourceContract}.
 */
public class TestRequiredSourceSettings {

  private static final String NAME = "X (mode blob)";
  private static final String START_COMMIT = HoodieStreamerConfig.START_COMMIT_BEFORE_TRANSFORMER.key();
  private static final String INJECTOR = HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key();
  private static final String CODEC = HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key();
  private static final String RECORD_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key();
  private static final String MARKERS = HoodieWriteConfig.MARKERS_TYPE.key();
  private static final String KEY = "a,b,c";

  /** Settings of every kind, as a source declares them. */
  private static RequiredSourceSettings settings() {
    return RequiredSourceSettings.builder(NAME)
        .requireIgnoringCase(HoodieStreamerConfig.START_COMMIT_BEFORE_TRANSFORMER, "true")
        .requireFields(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, Arrays.asList("a", "b", "c"), HoodieTableConfig.RECORDKEY_FIELDS)
        .requireIgnoringCase(HoodieWriteConfig.MARKERS_TYPE, "TIMELINE_SERVER_BASED")
        .require(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, "com.example.Injector")
        .hint("a job's own injector goes under com.example.delegate")
        .withDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed")
        .build();
  }

  private static String refusal(TypedProperties props) {
    return assertThrows(IllegalArgumentException.class, () -> settings().apply(props)).getMessage();
  }

  /** A source without a contract declares nothing: the merge returns the job's properties themselves. */
  @Test
  public void sourceWithoutAContractGivesNone() {
    TypedProperties props = new TypedProperties();
    props.setProperty(CODEC, "snappy");
    RequiredSourceSettings none = SourceContracts.NONE.requiredSettings(props);
    assertSame(props, none.apply(props));
    none.checkTable("file:/tables/t", table("x"));
  }

  /** A source's contract is asked with the job's own properties, and its settings are merged into a copy. */
  @Test
  public void contractSettingsAreApplied() {
    TypedProperties jobProps = new TypedProperties();
    jobProps.setProperty("hoodie.upsert.shuffle.parallelism", "2");
    TypedProperties before = TypedProperties.copy(jobProps);
    RecordingContract contract = new RecordingContract();
    TypedProperties merged = contract.requiredSettings(jobProps).apply(jobProps);
    assertSame(jobProps, contract.askedWith.get(), "the contract is asked with the job's own properties");
    assertEquals(before, jobProps, "the merge changed the job's properties");
    Map<String, String> expected = new HashMap<>();
    expected.put("hoodie.upsert.shuffle.parallelism", "2");
    expected.put(START_COMMIT, "true");
    expected.put(RECORD_KEY, KEY);
    expected.put(MARKERS, "TIMELINE_SERVER_BASED");
    expected.put(INJECTOR, "com.example.Injector");
    expected.put(CODEC, "uncompressed");
    assertEquals(expected, asMap(merged));
  }

  /** A refusal names the builder's name, which carries the source and its mode. */
  @Test
  public void refusalNamesBuilderName() {
    TypedProperties props = new TypedProperties();
    props.setProperty(START_COMMIT, "false");
    assertEquals(NAME + " requires " + START_COMMIT + "=true, configured: false", refusal(props));
    assertTrue(refusal(props).startsWith("X (mode blob) requires"));
  }

  /** The declaration is an instance method: nothing in the class looks a method up by name. */
  @Test
  public void noReflectionLeft() throws Exception {
    String source = new String(Files.readAllBytes(Paths.get("src/main/java/org/apache/hudi/utilities/sources/RequiredSourceSettings.java")),
        StandardCharsets.UTF_8);
    assertFalse(source.contains("getMethod"), "RequiredSourceSettings still looks a method up");
    assertFalse(source.contains("DECLARING_METHOD"));
    assertFalse(source.contains("Class.forName"));
  }

  /** A blank value is unset. */
  @Test
  public void blankValueIsUnset() {
    TypedProperties props = new TypedProperties();
    props.setProperty(RECORD_KEY, " ");
    props.setProperty(START_COMMIT, "");
    TypedProperties merged = settings().apply(props);
    assertEquals(KEY, merged.getString(RECORD_KEY));
    assertEquals("true", merged.getString(START_COMMIT));
  }

  /** The job's own value, the same up to case and spaces, is accepted and written in the required form. */
  @Test
  public void sameValueIsAcceptedInTheRequiredForm() {
    TypedProperties props = new TypedProperties();
    props.setProperty(START_COMMIT, "TRUE");
    props.setProperty(MARKERS, "timeline_server_based");
    props.setProperty(INJECTOR, " com.example.Injector");
    props.setProperty(RECORD_KEY, " a, b ,c ");
    TypedProperties merged = settings().apply(props);
    assertEquals("true", merged.getString(START_COMMIT));
    assertEquals("TIMELINE_SERVER_BASED", merged.getString(MARKERS));
    assertEquals("com.example.Injector", merged.getString(INJECTOR));
    assertEquals(KEY, merged.getString(RECORD_KEY));
  }

  /** A different value is refused, naming the source, the setting, its value, the hint and the job's value. */
  @Test
  public void differentValueIsRefusedByName() {
    TypedProperties key = new TypedProperties();
    key.setProperty(RECORD_KEY, "c,b,a");
    assertEquals(NAME + " requires " + RECORD_KEY + "=a,b,c (these fields, in this order), configured: c,b,a", refusal(key));
    TypedProperties injector = new TypedProperties();
    injector.setProperty(INJECTOR, "com.example.Other");
    assertEquals(NAME + " requires " + INJECTOR + "=com.example.Injector (a job's own injector goes under com.example.delegate), "
        + "configured: com.example.Other", refusal(injector));
  }

  /** A default is set when unset; any value the job sets is kept. */
  @Test
  public void defaultKeepsTheJobsValue() {
    TypedProperties props = new TypedProperties();
    props.setProperty(CODEC, "zstd");
    assertEquals("zstd", settings().apply(props).getString(CODEC));
  }

  /** A refusal names the key the job set, the main key or an alternative one. */
  @Test
  public void refusalNamesTheKeyTheJobSet() {
    RequiredSourceSettings settings = RequiredSourceSettings.builder(NAME)
        .require(HoodieStreamerConfig.CHECKPOINT_PROVIDER_PATH, "/checkpoints").build();
    String alternative = HoodieStreamerConfig.CHECKPOINT_PROVIDER_PATH.getAlternatives().get(0);
    TypedProperties props = new TypedProperties();
    props.setProperty(alternative, "/elsewhere");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> settings.apply(props));
    assertEquals(NAME + " requires " + HoodieStreamerConfig.CHECKPOINT_PROVIDER_PATH.key() + "=/checkpoints, configured: "
        + alternative + "=/elsewhere", refused.getMessage());
    TypedProperties same = new TypedProperties();
    same.setProperty(alternative, "/checkpoints");
    assertEquals("/checkpoints", settings.apply(same).getString(HoodieStreamerConfig.CHECKPOINT_PROVIDER_PATH.key()));
  }

  /** A start check runs on the merged properties and is given the builder's name. */
  @Test
  public void startCheckRunsOnTheMergedPropertiesWithTheName() {
    AtomicReference<String> seen = new AtomicReference<>();
    RequiredSourceSettings settings = RequiredSourceSettings.builder(NAME)
        .require(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, "com.example.Injector")
        .check((name, merged) -> seen.set(name + ":" + merged.getString(INJECTOR))).build();
    settings.apply(new TypedProperties());
    assertEquals(NAME + ":com.example.Injector", seen.get());
  }

  /** A setting declared twice is a programming error, reported by name. */
  @Test
  public void settingDeclaredTwiceIsReported() {
    IllegalStateException twice = assertThrows(IllegalStateException.class, () -> RequiredSourceSettings.builder(NAME)
        .require(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, "a")
        .require(HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, "b"));
    assertEquals(NAME + " declares " + INJECTOR + " twice", twice.getMessage());
  }

  /** An existing table must store the required field list; another list, or the same fields in another order, is refused. */
  @Test
  public void existingTableWithAnotherKeyListIsRefused() {
    RequiredSourceSettings settings = settings();
    settings.checkTable("file:/tables/t", table(" a , b,c"));
    for (String stored : new String[] {"a", "c,b,a", "a,b,c,d"}) {
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> settings.checkTable("file:/tables/t", table(stored)));
      assertEquals(NAME + " requires the table's " + HoodieTableConfig.RECORDKEY_FIELDS.key() + "=" + KEY
          + ": the table at file:/tables/t stores " + stored + "; write this job to a new table", refused.getMessage());
    }
  }

  private static HoodieTableConfig table(String recordKeyFields) {
    HoodieTableConfig table = new HoodieTableConfig();
    table.setValue(HoodieTableConfig.RECORDKEY_FIELDS, recordKeyFields);
    return table;
  }

  private static Map<String, String> asMap(TypedProperties props) {
    Map<String, String> map = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      map.put(key, props.getString(key));
    }
    return map;
  }

  /** A source contract that records the properties it was asked with. */
  private static final class RecordingContract implements SourceContract {
    private final AtomicReference<TypedProperties> askedWith = new AtomicReference<>();

    @Override
    public RequiredSourceSettings requiredSettings(TypedProperties jobProps) {
      askedWith.set(jobProps);
      return settings();
    }
  }
}
