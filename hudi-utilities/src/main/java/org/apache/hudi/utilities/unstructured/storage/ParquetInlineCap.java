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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;

import lombok.extern.slf4j.Slf4j;

/**
 * The inline threshold of a table written with a Parquet that cannot turn statistics off for one column (before 1.14, e.g. the
 * Spark 3.5 builds; {@link HoodieBaseParquetWriter#supportsColumnStatisticsSwitch()}). Such a Parquet keeps min/max statistics of
 * every inline value in memory while it writes, so the threshold of the blob table ({@code hoodie.blob.table.inline.max.bytes}),
 * and of an embedding input store defaults to
 * {@link #DEFAULT_BYTES} there, and a larger configured value is logged at WARN and replaced by {@link #MAX_BYTES}. With the
 * switch the configured value, or the key's own default, is used as is. The per-task heap-share ceiling of the blob-table and
 * embedding-input keys ({@link InlineCeiling}) applies on top of either.
 */
@Slf4j
public final class ParquetInlineCap {

  /** 10 KiB: the inline threshold on a Parquet without the switch when the job does not set one. */
  public static final long DEFAULT_BYTES = 10L * 1024;
  /** 80 KiB: the largest inline threshold used on a Parquet without the switch. */
  public static final long MAX_BYTES = 80L * 1024;

  private ParquetInlineCap() {
  }

  /**
   * The inline threshold a writer uses.
   *
   * @param key              the threshold's key, for the warning
   * @param configured       the value the job set, or null when it set none
   * @param defaultBytes     the key's default with the switch
   * @param statisticsSwitch whether this Parquet has the per-column statistics switch
   */
  public static long effective(String key, Long configured, long defaultBytes, boolean statisticsSwitch) {
    if (statisticsSwitch) {
      return configured == null ? defaultBytes : configured;
    }
    if (configured == null) {
      return DEFAULT_BYTES;
    }
    if (configured > MAX_BYTES) {
      log.warn(warning(key, configured));
      return MAX_BYTES;
    }
    return configured;
  }

  /** The warning for a configured threshold above {@link #MAX_BYTES}: the key, the value and the cap. */
  public static String warning(String key, long configured) {
    return key + "=" + configured + " is above " + MAX_BYTES + " bytes, the largest inline threshold on a Parquet without "
        + "per-column statistics (before 1.14, e.g. the Spark 3.5 builds): " + MAX_BYTES + " is used";
  }
}
