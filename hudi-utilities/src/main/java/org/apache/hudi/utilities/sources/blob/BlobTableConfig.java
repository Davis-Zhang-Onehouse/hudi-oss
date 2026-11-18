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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;
import org.apache.hudi.utilities.unstructured.storage.ParquetInlineCap;

import javax.annotation.concurrent.Immutable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Configuration for selecting object announcements and storing their bytes in a blob table. The unstructured data source owns
 * the upstream checkpoint, instant and byte budgets.
 * Inline values use the configured threshold or its default. The blob job's Parquet codec and bloom filter are set by its
 * source class when the job leaves them unset (uncompressed, no bloom filter); any value the job sets is kept.
 */
@Immutable
@ConfigClassProperty(name = "Blob Table Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling how unstructured files are ingested into a Hudi blob table.")
public class BlobTableConfig extends HoodieConfig {

  public static final String PREFIX = "hoodie.blob.table.";

  /**
   * Small blob threshold: a file of at most this many bytes is stored INLINE in the data file, a larger one is copied
   * out of line. The declared 1 MiB default is effective on Parquet 1.14+ (the Spark 4 builds). On Spark 3.5's older Parquet,
   * which cannot disable statistics per column, the effective default is 10 KiB and configured values are capped at 80 KiB
   * ({@link ParquetInlineCap}). When this key is unset, upgrading an existing table's writer from Spark 3.5 to Spark 4 raises
   * its effective threshold from 10 KiB to 1 MiB: subsequent writes store files larger than 10 KiB and at most 1 MiB inline,
   * where they were stored out of line before the upgrade. Previously stored values keep their existing storage shape.
   * Bounded above by {@link BlobInlineCeiling}. Read it with {@link #inlineMaxBytes(TypedProperties)}.
   */
  public static final ConfigProperty<Long> INLINE_MAX_BYTES = ConfigProperty
      .key(PREFIX + "inline.max.bytes")
      .defaultValue(1024L * 1024L)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("Files with size <= this many bytes are stored inline in the blob column; larger files are "
          + "copied to the table's .hoodie/.blob folder and referenced out of line. The declared default is 1 MiB (1048576 bytes), "
          + "effective on Parquet 1.14+ (the Spark 4 builds). On Spark 3.5's Parquet older than 1.14, which cannot turn off min/max "
          + "statistics for one column, the effective default is 10 KiB (10240 bytes); a configured value above 80 KiB (81920 bytes) "
          + "is logged at WARN and capped at 81920. With this key unset, upgrading an existing table's writer from Spark 3.5 to "
          + "Spark 4 raises its effective threshold from 10 KiB to 1 MiB. Subsequent writes store files larger than 10 KiB and at "
          + "most 1 MiB inline instead of out of line; previously stored values keep their existing storage shape. "
          + "A value above the per-task inline ceiling ("
          + PREFIX + "inline.max.heap.fraction x the per-task heap share) is refused at the job's first round, before the round "
          + "reads the events table's rows or starts its instant, unless "
          + PREFIX + "inline.ceiling.enforce is false.");

  /**
   * The inline threshold a blob round uses: {@link #INLINE_MAX_BYTES} as the job sets it, or its default, on a Parquet with the
   * per-column statistics switch; without it 10 KiB by default and at most 80 KiB ({@link ParquetInlineCap}). For an unset key,
   * a Spark 3.5-to-4 writer upgrade raises the effective threshold to 1 MiB even when writing an existing table.
   */
  public static long inlineMaxBytes(TypedProperties props) {
    return inlineMaxBytes(props, HoodieBaseParquetWriter.supportsColumnStatisticsSwitch());
  }

  /**
   * As {@link #inlineMaxBytes(TypedProperties)}, for a Parquet with or without the per-column statistics switch.
   *
   * @throws IllegalArgumentException naming the key when the configured value is not in [0, 2^31-9]
   */
  public static long inlineMaxBytes(TypedProperties props, boolean statisticsSwitch) {
    Long configured = ConfigUtils.containsConfigProperty(props, INLINE_MAX_BYTES)
        ? ConfigUtils.getLongWithAltKeys(props, INLINE_MAX_BYTES) : null;
    if (configured != null && (configured < 0 || configured > Integer.MAX_VALUE - 8)) {
      throw new IllegalArgumentException(INLINE_MAX_BYTES.key() + " must be in [0, 2^31-9]: " + configured);
    }
    return ParquetInlineCap.effective(INLINE_MAX_BYTES.key(), configured, INLINE_MAX_BYTES.defaultValue(), statisticsSwitch);
  }

  /**
   * Fraction of the per-task heap share that the inline threshold may reach (see {@link BlobInlineCeiling}). The default 1/64 keeps
   * the modeled per-task memory of a blob round (about 112 MiB that does not depend on the threshold plus 50 times the threshold for
   * two open file writers) under the per-task share once that share is at least 511 MiB.
   */
  public static final ConfigProperty<Double> INLINE_MAX_HEAP_FRACTION = ConfigProperty
      .key(PREFIX + "inline.max.heap.fraction")
      .defaultValue(1.0 / 64)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("Fraction in (0, 1] of the per-task heap share up to which " + PREFIX + "inline.max.bytes may be set. "
          + "Per-task share = (executor heap - Spark's reserved system memory, 300 MiB outside tests) / task slots, task slots = "
          + "executor cores / spark.task.cpus; in local mode the executor heap is the driver JVM's maximum heap and the cores are "
          + "the master's thread count, otherwise spark.executor.memory and spark.executor.cores (default 1: set it when executors "
          + "run more than one task). A threshold above fraction x share is refused at the job's first round and again in a task "
          + "before its first inline read, naming both numbers. The default 1/64 keeps the modeled per-task memory of a round "
          + "(about 112 MiB independent of the threshold plus 50 x the threshold) under the share for shares of 511 MiB or more. "
          + "The default 1 MiB threshold needs a share of at least 64 MiB (heap >= 300 MiB + task slots x 64 MiB): below that the "
          + "defaults refuse each other and " + PREFIX + "inline.max.bytes must be lowered. local[*] counts every core of the "
          + "machine, so a small heap on a many-core host is such a case; use local[N].");

  /**
   * The only way past the inline ceiling: with it off a threshold above the ceiling is logged at WARN and used as configured.
   */
  public static final ConfigProperty<Boolean> INLINE_CEILING_ENFORCE = ConfigProperty
      .key(PREFIX + "inline.ceiling.enforce")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("When true (default), an " + PREFIX + "inline.max.bytes above the per-task inline ceiling ("
          + PREFIX + "inline.max.heap.fraction x the per-task heap share) fails the job before any file is read. When false, "
          + "it is only logged at WARN and the threshold is used as configured: the blob job's memory is then no longer "
          + "hard capped, and a file up to the threshold can exhaust a task's heap.");

  /**
   * Extensions (without the dot) of the object keys that are ingested, matched case-sensitively; every other announced object
   * is not ingested (it gets no row at all).
   */
  public static final ConfigProperty<String> FILE_EXTENSIONS = ConfigProperty
      .key(PREFIX + "file.extensions")
      .defaultValue("pdf,png,jpg,jpeg,gif,bmp,tif,tiff,mp4,mov,avi,mkv,webm")
      .sinceVersion("1.3.0")
      .withDocumentation("Comma-separated file extensions (without the dot) of the object keys to ingest: pdf, image and "
          + "video files by default. Matched case-sensitively against the end of the key, dot included: a key ending in "
          + ".JPG is not ingested. The list may only narrow the default: an empty list, or one naming any other extension, is "
          + "refused when the source starts.");

  /** Buffer used by each bounded out-of-line streaming copy. */
  public static final ConfigProperty<Integer> COPY_BUFFER_BYTES = ConfigProperty
      .key(PREFIX + "copy.buffer.bytes")
      .defaultValue(64 * 1024)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("Buffer size in bytes used to stream a file larger than the inline threshold into the table.");

  /**
   * Tries in all of one out-of-line copy that fails with a transient storage fault, within the same round;
   * the last failure fails the round, which the next run retries from the same checkpoint. A content fault and an access
   * denial are never retried.
   */
  public static final ConfigProperty<Integer> COPY_RETRY_MAX_ATTEMPTS = ConfigProperty
      .key(PREFIX + "copy.retry.max.attempts")
      .defaultValue(3)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("Tries in all of an out-of-line copy that fails with a transient storage fault (a throttled, reset or "
          + "timed-out request) within the same round; the last failure fails the round, which the next run retries from the "
          + "same checkpoint. Must be positive.");

  /** The wait before the second try of a copy ({@link #COPY_RETRY_MAX_ATTEMPTS}); each later wait doubles, capped at 30 s. */
  public static final ConfigProperty<Long> COPY_RETRY_BACKOFF_MS = ConfigProperty
      .key(PREFIX + "copy.retry.backoff.ms")
      .defaultValue(1000L)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("Wait in ms before the second try of an out-of-line copy after a transient storage fault; each later "
          + "wait doubles, capped at 30 s. Must not be negative.");

  /**
   * The in-commit retry of a transient copy fault ({@link #COPY_RETRY_MAX_ATTEMPTS}, {@link #COPY_RETRY_BACKOFF_MS}).
   *
   * @throws BlobSourceConfigException naming the key, for attempts below 1 or a negative backoff
   */
  public static TransientRetry copyRetry(Properties props) {
    int attempts = intValue(props, COPY_RETRY_MAX_ATTEMPTS);
    String backoffText = ConfigUtils.getStringWithAltKeys(props::getProperty, COPY_RETRY_BACKOFF_MS);
    long backoff;
    try {
      backoff = backoffText == null ? COPY_RETRY_BACKOFF_MS.defaultValue() : Long.parseLong(backoffText.trim());
    } catch (NumberFormatException e) {
      throw new BlobSourceConfigException(COPY_RETRY_BACKOFF_MS.key() + " must be a number of ms: " + backoffText, e);
    }
    if (attempts < 1) {
      throw new BlobSourceConfigException(COPY_RETRY_MAX_ATTEMPTS.key() + " must be positive: " + attempts);
    }
    if (backoff < 0) {
      throw new BlobSourceConfigException(COPY_RETRY_BACKOFF_MS.key() + " must not be negative: " + backoff);
    }
    return new TransientRetry(attempts, backoff);
  }

  /**
   * The copy buffer ({@link #COPY_BUFFER_BYTES}).
   *
   * @throws BlobSourceConfigException naming the key, for a value below 1
   */
  public static int copyBufferBytes(Properties props) {
    int bytes = intValue(props, COPY_BUFFER_BYTES);
    if (bytes <= 0) {
      throw new BlobSourceConfigException(COPY_BUFFER_BYTES.key() + " must be positive: " + bytes);
    }
    return bytes;
  }

  private static int intValue(Properties props, ConfigProperty<Integer> property) {
    String raw = ConfigUtils.getStringWithAltKeys(props, property, String.valueOf(property.defaultValue()));
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      throw new BlobSourceConfigException(property.key() + " must be an integer, got '" + raw + "'", e);
    }
  }

  /** The configured extensions, trimmed, with blanks dropped and case kept. */
  public static List<String> fileExtensions(TypedProperties props) {
    return splitList(ConfigUtils.getStringWithAltKeys(props, FILE_EXTENSIONS, true));
  }

  private static List<String> splitList(String value) {
    return value == null ? Collections.emptyList() : Arrays.stream(value.split(","))
        .map(String::trim).filter(entry -> !entry.isEmpty()).collect(Collectors.toList());
  }

}
