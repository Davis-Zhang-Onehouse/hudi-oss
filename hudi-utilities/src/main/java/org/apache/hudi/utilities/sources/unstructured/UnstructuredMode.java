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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.transform.Transformer;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The job of an unstructured data source ({@link UnstructuredSourceConfig#MODE}): which table it writes, the transformer the
 * streamer runs in every round, the transformers it refuses, and the checks of the job's own properties that belong to that job.
 */
public enum UnstructuredMode {

  /** Writes the blob table, through {@link BlobQuarantineTransformer}. */
  BLOB("blob", BlobTableSchema.KEY_COLUMNS, BlobQuarantineTransformer.class);

  private final String configValue;
  private final List<String> keyColumns;
  private final Class<? extends Transformer> requiredTransformer;

  UnstructuredMode(String configValue, List<String> keyColumns, Class<? extends Transformer> requiredTransformer) {
    this.configValue = configValue;
    this.keyColumns = keyColumns;
    this.requiredTransformer = requiredTransformer;
  }

  /**
   * The job {@link UnstructuredSourceConfig#MODE} names.
   *
   * @throws IllegalArgumentException naming the key and its values when it is unset or another value
   */
  public static UnstructuredMode from(TypedProperties props) {
    return UnstructuredSourceKeys.read(props, UnstructuredSourceConfig.MODE, values(), UnstructuredMode::configValue);
  }

  /** The value of {@link UnstructuredSourceConfig#MODE} that names this job. */
  public String configValue() {
    return configValue;
  }

  /** The record key columns of the job's table, in order. */
  public List<String> keyColumns() {
    return keyColumns;
  }

  /** The transformer the streamer runs in every round of this job. */
  public Class<? extends Transformer> requiredTransformer() {
    return requiredTransformer;
  }

  /** The transformers this job refuses in its chain: none, while it is the only job. */
  public Set<Class<? extends Transformer>> excludedTransformers() {
    return Collections.emptySet();
  }

  /**
   * Checks the job's own properties that only this job reads, and changes none of them. Refusals name {@code sourceName}.
   *
   * @throws IllegalArgumentException when a setting has a value this job cannot run with
   */
  public void validate(TypedProperties props, String sourceName) {
    switch (this) {
      case BLOB:
        break;
      default:
        throw new UnsupportedOperationException("Unknown unstructured mode " + this);
    }
  }
}
