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

  /** Files at most this size are stored inline. */
  public static final ConfigProperty<Long> INLINE_MAX_BYTES = ConfigProperty
      .key(PREFIX + "inline.max.bytes")
      .defaultValue(1024L * 1024L)
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("Files with size <= this many bytes are stored inline in the blob column. At most 2147483639, "
          + "the largest byte array an inline value is read into; a larger value is refused when the source starts.");

  /** The largest inline threshold: an inline value is read into one byte array, whose length is an int. */
  public static final long MAX_INLINE_MAX_BYTES = Integer.MAX_VALUE - 8;

  /** The configured inline threshold, or its ordinary default; refuses a value no byte array can hold. */
  public static long inlineMaxBytes(TypedProperties props) {
    long bytes = ConfigUtils.getLongWithAltKeys(props, INLINE_MAX_BYTES);
    if (bytes > MAX_INLINE_MAX_BYTES) {
      throw new IllegalArgumentException(INLINE_MAX_BYTES.key() + " must be at most " + MAX_INLINE_MAX_BYTES
          + " (the largest byte array an inline value is read into): " + bytes);
    }
    return bytes;
  }

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

  /** Returns the positive copy buffer size, naming the setting when its value is invalid. */
  public static int copyBufferBytes(Properties props) {
    String raw = ConfigUtils.getStringWithAltKeys(props, COPY_BUFFER_BYTES, String.valueOf(COPY_BUFFER_BYTES.defaultValue()));
    int bytes;
    try {
      bytes = Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(COPY_BUFFER_BYTES.key() + " must be an integer: " + raw, e);
    }
    if (bytes <= 0) {
      throw new IllegalArgumentException(COPY_BUFFER_BYTES.key() + " must be positive: " + bytes);
    }
    return bytes;
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
