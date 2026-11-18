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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;

/**
 * The preparation contract persisted at table creation and stamped on every row. Versions include resolved implementation
 * identities, runtime libraries and effective output-changing configuration, including defaults. Performance settings are
 * excluded. Missing table pins are refused, never inferred or upgraded in place. Bundled libraries are identified by the jar
 * the class path holds, the same on the driver and the executors; FFmpeg, which only the executors hold, by the versions this
 * build declares ({@link FfmpegLibraryVersions}), which each executor checks its jars against before decoding.
 */
public final class PrepLogicVersions {

  private static final String PREFIX = "hoodie.table.embedding.prep.";
  private static final Map<String, String> LIBRARIES = new ConcurrentHashMap<>();
  /** The pinned value of an output setting that has no default and is not configured. */
  static final String UNSET = "<unset>";
  private static final Map<String, String> DEPENDENCIES = Map.of(
      "com/knuddels/jtokkit/Encodings.class", "com.knuddels/jtokkit",
      "org/apache/pdfbox/util/Version.class", "org.apache.pdfbox/pdfbox",
      "org/jcodec/api/FrameGrab.class", "org.jcodec/jcodec",
      "org/jcodec/scale/AWTUtil.class", "org.jcodec/jcodec-javase",
      "io/github/givimad/whisperjni/WhisperJNI.class", "io.github.givimad/whisper-jni");
  private static final ConfigProperty<?>[] OUTPUT_SETTINGS = {
      EmbeddingTableConfig.CHUNK_MAX_TOKENS, EmbeddingTableConfig.CHUNK_MAX_WORDS,
      EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS, EmbeddingTableConfig.CHUNK_SENTENCE_LOCALE,
      EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE, EmbeddingTableConfig.TOKENIZER_TYPE,
      EmbeddingTableConfig.TOKENIZER_ENCODING, EmbeddingTableConfig.PDF_MAX_INPUT_BYTES,
      EmbeddingTableConfig.PDF_MAX_PAGES, EmbeddingTableConfig.PDF_MAX_PAGE_CHARS,
      EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS, EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES,
      EmbeddingTableConfig.PDF_MAX_PAGE_FONT_BYTES, EmbeddingTableConfig.PDF_MAX_GRAPHICS_DEPTH,
      EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS, EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES,
      EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES, EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES,
      EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS, EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD,
      EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE, EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS,
      AudioConfigKeys.ENABLED, AudioConfigKeys.SAMPLE_RATE, AudioConfigKeys.VAD_THRESHOLD_DB, AudioConfigKeys.VAD_MIN_SILENCE_MS,
      AudioConfigKeys.SEGMENT_MAX_MS, AudioConfigKeys.SEGMENT_MAX_BYTES, AudioConfigKeys.MAX_DURATION_MS
  };

  private final Map<String, String> versions;

  private PrepLogicVersions(Map<String, String> versions) {
    this.versions = versions;
  }

  /** Resolves identities without opening source files or loading optional native libraries. */
  public static PrepLogicVersions from(TypedProperties props) {
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      Map<String, String> settings = effectiveSettings(props);
      Tokenizer tokenizer = TokenizerFactory.create(props);
      Map<String, String> result = new LinkedHashMap<>();
      for (String kind : Arrays.asList("text", "image", "video", "audio")) {
        Map<String, String> identity = new TreeMap<>();
        identity.put("format", "prep-logic-v1");
        identity.put("jdk", System.getProperty("java.runtime.version") + "/" + System.getProperty("java.vendor"));
        settings.forEach((key, value) -> {
          if (applies(kind, key)) {
            identity.put(key, value);
          }
        });
        for (Map.Entry<String, String> entry : registry.classNamesByExtension().entrySet()) {
          FilePreprocessor processor = registry.forExtension(entry.getKey()).get();
          if (kind.equals(kindOf(processor.sourceKind()))) {
            identity.put("extension." + entry.getKey(), entry.getValue() + ";" + processor.name());
          }
        }
        if (kind.equals("text") || kind.equals("audio")) {
          identity.put("normalizer", TextNormalizer.LOGIC_VERSION);
          identity.put("tokenizer", tokenizer.id());
          identity.put("jtokkit", library("com/knuddels/jtokkit/Encodings.class"));
        }
        if (kind.equals("text")) {
          identity.put("pdfbox", library("org/apache/pdfbox/util/Version.class"));
        } else if (kind.equals("video")) {
          identity.put("ffmpeg.logic", FfmpegKeyframeVideoFrameSource.LOGIC_VERSION);
          identity.put("jcodec.logic", JcodecKeyframeVideoFrameSource.LOGIC_VERSION);
          identity.put("ffmpeg", FfmpegLibraryVersions.FFMPEG);    // declared: executors only hold the jars
          identity.put("javacv", FfmpegLibraryVersions.JAVACV);
          identity.put("jcodec", library("org/jcodec/api/FrameGrab.class"));
          identity.put("jcodec.javase", library("org/jcodec/scale/AWTUtil.class"));
        } else if (kind.equals("audio")) {
          AudioConfig audio = AudioConfig.from(props);
          identity.put("enabled", String.valueOf(audio.enabled()));
          if (audio.enabled()) {
            identity.putAll(audio.logicStyle(tokenizer));
            identity.put("ffmpeg", FfmpegLibraryVersions.FFMPEG);
            identity.put("javacv", FfmpegLibraryVersions.JAVACV);
            if (ConfigUtils.getStringWithAltKeys(props, AudioConfigKeys.TRANSCRIBER_CLASS,
                AudioConfig.DEFAULT_TRANSCRIBER_CLASS).trim().equals(AudioConfig.DEFAULT_TRANSCRIBER_CLASS)) {
              identity.put("whisper.jni", library("io/github/givimad/whisperjni/WhisperJNI.class"));
              identity.put("whisper.adapter", WhisperSegmentTranscriber.IMPLEMENTATION_VERSION);
            }
          }
        }
        result.put(kind, "prep-logic-v1;sha256=" + digest(identity));
      }
      return new PrepLogicVersions(result);
    } finally {
      registry.close();
    }
  }

  /** The expected table properties; also used to build exact existing-table checks. */
  public void requireTableProperties(UnstructuredTableProperties.Builder settings) {
    versions.forEach((kind, version) -> settings.requireTableProperty(property(kind), version));
  }

  /** Carries the driver-validated contracts into each executor without changing the caller's job settings. */
  public TypedProperties executionProperties(TypedProperties props) {
    TypedProperties execution = TypedProperties.copy(props);
    versions.forEach((kind, version) -> execution.setProperty(property(kind).key(), version));
    return execution;
  }

  /** Refuses executor runtime/library drift before any file is read. Standalone iterators may omit execution pins. */
  public void checkExecution(TypedProperties props) {
    if (versions.keySet().stream().noneMatch(kind -> props.containsKey(property(kind).key()))) {
      return;
    }
    versions.forEach((kind, actual) -> {
      String expected = props.getProperty(property(kind).key());
      if (!actual.equals(expected)) {
        throw new IllegalArgumentException("Embedding prep executor " + kind
            + " logic version does not match the driver/table contract; nothing was prepared");
      }
    });
  }

  /** Refuses a missing or different pin before a direct transformer invocation prepares any rows. */
  public void checkTable(String basePath, HoodieTableConfig table) {
    UnstructuredTableProperties.Builder settings = UnstructuredTableProperties.builder("Embedding prep");
    requireTableProperties(settings);
    settings.build().checkTable(basePath, table);
  }

  /** The table-pinned version for the source kind; video speech uses the audio contract. */
  public String forRow(SourceKind kind, String modality) {
    return versions.get(kind == SourceKind.VIDEO && "text".equals(modality) ? "audio" : kindOf(kind));
  }

  /** The expected version of text, image, video or audio preparation. */
  public String version(String kind) {
    if (!versions.containsKey(kind)) {
      throw new IllegalArgumentException("Unknown prep source kind " + kind);
    }
    return versions.get(kind);
  }

  private static ConfigProperty<String> property(String kind) {
    return ConfigProperty.key(PREFIX + kind + ".logic.version").noDefaultValue();
  }

  private static String kindOf(SourceKind kind) {
    switch (kind) {
      case DOCUMENT:
        return "text";
      case IMAGE:
        return "image";
      case VIDEO:
        return "video";
      default:
        throw new UnsupportedOperationException("Unknown prep source kind " + kind);
    }
  }

  private static Map<String, String> effectiveSettings(TypedProperties props) {
    Map<String, String> settings = new TreeMap<>();
    for (ConfigProperty<?> key : OUTPUT_SETTINGS) {
      // A key without a default is pinned as "<unset>" when not configured.
      String unset = key.hasDefaultValue() ? String.valueOf(key.defaultValue()) : UNSET;
      settings.put(key.key(), ConfigUtils.getStringWithAltKeys(props, key, unset).trim());
    }
    int maxTokens = Integer.parseInt(settings.get(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()));
    settings.put(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), ConfigUtils.getStringWithAltKeys(props,
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS, String.valueOf(Math.min(EmbeddingTableConfig.DEFAULT_CHUNK_OVERLAP_TOKENS, maxTokens / 2))).trim());
    for (String key : props.stringPropertyNames()) {
      if ((key.startsWith(EmbeddingTableConfig.CHUNK_PREFIX) || key.startsWith(EmbeddingTableConfig.PDF_PREFIX)
          || key.startsWith(EmbeddingTableConfig.PREP_PREFIX))
          && !performanceSetting(key) && !EmbeddingTableConfig.isPreprocessorClassKey(key)
          && !key.equals(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key())) {
        settings.put(key, props.getProperty(key).trim());
      }
    }
    settings.put(AudioConfigKeys.ENABLED.key(), String.valueOf(AudioConfig.from(props).enabled()));
    return settings;
  }

  private static boolean performanceSetting(String key) {
    return key.equals(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key())
        || key.startsWith(EmbeddingTableConfig.BALANCE_PREFIX)
        || key.startsWith(EmbeddingTableConfig.PREP_PREFIX + "source.retry.")
        || key.equals(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key())
        || key.equals(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key())
        || key.equals(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key());
  }

  private static boolean applies(String kind, String key) {
    if (key.startsWith(EmbeddingTableConfig.CHUNK_PREFIX)) {
      return kind.equals("text") || kind.equals("audio");
    }
    if (key.startsWith(EmbeddingTableConfig.PDF_PREFIX)) {
      return kind.equals("text");
    }
    if (key.startsWith(EmbeddingTableConfig.PREP_PREFIX + "image.")) {
      return kind.equals("image");
    }
    if (key.startsWith(EmbeddingTableConfig.PREP_PREFIX + "video.")) {
      return kind.equals("video");
    }
    if (key.startsWith(EmbeddingTableConfig.PREP_PREFIX + "audio.") || key.startsWith(AudioConfigKeys.RUNTIME_PREFIX)) {
      return kind.equals("audio");
    }
    return true;
  }

  private static String digest(Map<String, String> identity) {
    MessageDigest digest = sha256();
    identity.forEach((key, value) -> digest.update((key.length() + ":" + key + value.length() + ":" + value)
        .getBytes(StandardCharsets.UTF_8)));
    return hex(digest.digest());
  }

  /**
   * Reads dependency-owned Maven metadata without initializing the library. The outer manifest may describe a shaded
   * application bundle, so it is never used as the dependency version. A jar without dependency metadata is pinned by bytes.
   */
  private static String library(String resource) {
    return LIBRARIES.computeIfAbsent(resource, key -> {
      URL url = PrepLogicVersions.class.getClassLoader().getResource(key);
      if (url == null) {
        return "absent";
      }
      try {
        if (url.openConnection() instanceof JarURLConnection) {
          JarURLConnection jar = (JarURLConnection) url.openConnection();
          String coordinate = DEPENDENCIES.get(key);
          JarEntry entry = coordinate == null ? null
              : jar.getJarFile().getJarEntry("META-INF/maven/" + coordinate + "/pom.properties");
          if (entry != null) {
            Properties metadata = new Properties();
            try (InputStream in = jar.getJarFile().getInputStream(entry)) {
              metadata.load(in);
            }
            String version = metadata.getProperty("version", "").trim();
            if (!version.isEmpty()) {
              return coordinate + ":" + version;
            }
          }
          url = jar.getJarFileURL();
        }
        MessageDigest digest = sha256();
        try (InputStream in = url.openStream()) {
          byte[] buffer = new byte[8192];
          int read;
          while ((read = in.read(buffer)) != -1) {
            digest.update(buffer, 0, read);
          }
        }
        return "sha256=" + hex(digest.digest());
      } catch (IOException e) {
        throw new HoodieIOException("Cannot identify preparation library " + resource, e);
      }
    });
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  private static String hex(byte[] digest) {
    StringBuilder hex = new StringBuilder();
    for (byte value : digest) {
      hex.append(String.format("%02x", value));
    }
    return hex.toString();
  }
}
