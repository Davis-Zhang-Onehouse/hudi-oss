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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Exact table pins owned by unstructured streams, checked before preparing or reading input. */
public final class UnstructuredTableProperties {
  private final String sourceName;
  private final Map<String, String> pins;

  private UnstructuredTableProperties(String sourceName, Map<String, String> pins) {
    this.sourceName = sourceName;
    this.pins = new LinkedHashMap<>(pins);
  }

  public static Builder builder(String sourceName) {
    return new Builder(sourceName);
  }

  /** Adds the creation pins to a copy of the job, refusing an explicitly different encoding or layout. */
  public TypedProperties apply(TypedProperties props) {
    TypedProperties merged = TypedProperties.copy(props);
    pins.forEach((key, value) -> {
      String configured = props.getProperty(key);
      if (configured != null && !value.equals(configured)) {
        throw new IllegalArgumentException(sourceName + " requires " + key + "=" + value + ", configured: " + configured);
      }
      merged.setProperty(key, value);
    });
    return merged;
  }

  /** Refuses missing or different persisted pins; table properties are never inferred from the current job. */
  public void checkTable(String basePath, HoodieTableConfig table) {
    pins.forEach((key, value) -> {
      String stored = table.getProps().getProperty(key);
      if (!value.equals(stored)) {
        throw new IllegalArgumentException(sourceName + " requires the table's " + key + "=" + value
            + ": the table at " + basePath + " stores " + (stored == null ? "none" : stored) + "; write this job to a new table");
      }
    });
  }

  /** Opaque properties passed to Hudi's generic table creation mechanism. */
  public Properties tableProperties() {
    Properties properties = new Properties();
    properties.putAll(pins);
    return properties;
  }

  public static final class Builder {
    private final String sourceName;
    private final Map<String, String> pins = new LinkedHashMap<>();

    private Builder(String sourceName) {
      this.sourceName = sourceName;
    }

    public Builder requireTableProperty(ConfigProperty<String> key, String value) {
      pins.put(key.key(), value);
      return this;
    }

    public UnstructuredTableProperties build() {
      return new UnstructuredTableProperties(sourceName, pins);
    }
  }
}
