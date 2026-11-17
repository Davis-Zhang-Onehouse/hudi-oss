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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.sources.RequiredSourceSettings;
import org.apache.hudi.utilities.sources.blob.BlobCopyTransformer;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The job of an unstructured data source ({@link UnstructuredSourceConfig#MODE}): which table it writes, the transformer the
 * streamer runs in every round, the transformer it refuses, and the checks of the job's own properties that belong to that job.
 */
public enum UnstructuredMode {

  /** Writes the blob table, through {@link BlobCopyTransformer}. */
  BLOB("blob", BlobTableSchema.KEY_COLUMNS, BlobCopyTransformer.class, EmbeddingTableTransformer.class),
  /** Writes the embedding table, through {@link EmbeddingTableTransformer}. */
  EMBEDDING("embedding", EmbeddingTableSchema.KEY_COLUMNS, EmbeddingTableTransformer.class, BlobCopyTransformer.class);

  private final String configValue;
  private final List<String> keyColumns;
  private final Class<? extends Transformer> requiredTransformer;
  private final Class<? extends Transformer> excludedTransformer;

  UnstructuredMode(String configValue, List<String> keyColumns, Class<? extends Transformer> requiredTransformer,
                   Class<? extends Transformer> excludedTransformer) {
    this.configValue = configValue;
    this.keyColumns = keyColumns;
    this.requiredTransformer = requiredTransformer;
    this.excludedTransformer = excludedTransformer;
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

  /** The other job's transformer, which this job refuses in its chain. */
  public Set<Class<? extends Transformer>> excludedTransformers() {
    return Collections.singleton(excludedTransformer);
  }

  /**
   * Adds the blob job's settings to {@code settings}: for its stored-as-is bytes
   * {@code hoodie.parquet.compression.codec=uncompressed} and {@code hoodie.parquet.bloom.filter.enabled=false}, two defaults a job
   * may set to any value. The embedding job keeps Hudi's codec and bloom filter.
   */
  public RequiredSourceSettings.Builder addSettings(RequiredSourceSettings.Builder settings) {
    if (this == BLOB) {
      settings.withDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed")
          .withDefault(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED, "false");
    }
    return settings;
  }

  /**
   * Checks the job's own properties that only this job reads, and changes none of them. Refusals name {@code sourceName}.
   *
   * @throws IllegalArgumentException when a setting has a value this job cannot run with
   */
  public void validate(TypedProperties props, String sourceName) {
    Object embeddingMode = props.get(EmbeddingTableConfig.MODE.key());
    boolean embeddingModeSet = embeddingMode != null && !StringUtils.isNullOrEmpty(embeddingMode.toString().trim());
    switch (this) {
      case BLOB:
        if (embeddingModeSet) {
          throw new IllegalArgumentException(sourceName + " refuses " + EmbeddingTableConfig.MODE.key() + "=" + embeddingMode
              + ": it belongs to " + UnstructuredSourceConfig.MODE.key() + "=" + EMBEDDING.configValue + "; remove it");
        }
        // the copy buffer drives the out-of-line copies, which only the copy transformer makes
        BlobTableConfig.copyBufferBytes(props);
        break;
      case EMBEDDING:
        if (!embeddingModeSet) {
          throw new IllegalArgumentException(sourceName + " requires " + EmbeddingTableConfig.MODE.key() + ", one of: "
              + EmbeddingTableConfig.MODE_PREP_ONLY + ", " + EmbeddingTableConfig.MODE_PREP_AND_COMPUTE + "; configured: none");
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown unstructured mode " + this);
    }
  }
}
