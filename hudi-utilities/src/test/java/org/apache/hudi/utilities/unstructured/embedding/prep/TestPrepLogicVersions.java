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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableProperties;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableVersion;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableChecks;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestPrepLogicVersions {
  @TempDir
  Path temp;

  @Test
  void bundledDependenciesKeepTheirOwnVersionsUnderTheSameOuterManifest() throws Exception {
    String resource = "org/jcodec/api/FrameGrab.class";
    assertEquals(bundledIdentity(resource, "org.jcodec/jcodec", "1", null),
        bundledIdentity(resource, "org.jcodec/jcodec", "1", null));
    assertNotEquals(bundledIdentity(resource, "org.jcodec/jcodec", "1", null),
        bundledIdentity(resource, "org.jcodec/jcodec", "2", null));
  }

  @Test
  void videoPinsTheSeparatelyShippedJcodecImageConversionLibrary() throws Exception {
    String resource = "org/jcodec/scale/AWTUtil.class";
    assertEquals(bundledIdentity(resource, "org.jcodec/jcodec-javase", "1", "text"),
        bundledIdentity(resource, "org.jcodec/jcodec-javase", "2", "text"));
    assertNotEquals(bundledIdentity(resource, "org.jcodec/jcodec-javase", "1", "video"),
        bundledIdentity(resource, "org.jcodec/jcodec-javase", "2", "video"));
  }

  /** A real shaded-JAR resource lookup: one stable outer version, independently versioned dependency metadata. */
  private String bundledIdentity(String resource, String coordinate, String version, String kind) throws Exception {
    Path jar = Files.createTempFile(temp, "bundle-", ".jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "same-hudi-snapshot");
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
      out.putNextEntry(new JarEntry(resource));
      try (InputStream original = PrepLogicVersions.class.getClassLoader().getResourceAsStream(resource)) {
        out.write(original.readAllBytes());
      }
      out.closeEntry();
      out.putNextEntry(new JarEntry("META-INF/maven/" + coordinate + "/pom.properties"));
      out.write(("version=" + version + "\n").getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    ClassLoader parent = PrepLogicVersions.class.getClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {jar.toUri().toURL()}, parent) {
      @Override
      public URL getResource(String name) {
        return resource.equals(name) ? findResource(name) : super.getResource(name);
      }

      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!PrepLogicVersions.class.getName().equals(name) && !name.startsWith(PrepLogicVersions.class.getName() + "$")) {
          return super.loadClass(name, resolve);
        }
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          try (InputStream bytes = parent.getResourceAsStream(name.replace('.', '/') + ".class")) {
            byte[] code = bytes.readAllBytes();
            loaded = defineClass(name, code, 0, code.length);
          } catch (java.io.IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }) {
      Class<?> composer = loader.loadClass(PrepLogicVersions.class.getName());
      if (kind != null) {
        Object versions = composer.getMethod("from", TypedProperties.class).invoke(null, new TypedProperties());
        return (String) composer.getMethod("version", String.class).invoke(versions, kind);
      }
      Method library = composer.getDeclaredMethod("library", String.class);
      library.setAccessible(true);
      return (String) library.invoke(null, resource);
    }
  }

  @Test
  void implicitAndExplicitDefaultsShareAContract() {
    TypedProperties implicit = new TypedProperties();
    TypedProperties explicit = new TypedProperties();
    explicit.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "256");
    explicit.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "128");
    explicit.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), "10000");
    explicit.setProperty(AudioConfigKeys.ENABLED.key(), "false");
    explicit.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), VideoFramePreprocessor.class.getName());
    for (String kind : new String[] {"text", "image", "video", "audio"}) {
      assertEquals(PrepLogicVersions.from(implicit).version(kind), PrepLogicVersions.from(explicit).version(kind), kind);
    }
  }

  @Test
  void outputChangesInvalidateOnlyTheirSourceKinds() {
    TypedProperties original = new TypedProperties();
    TypedProperties changed = new TypedProperties();
    changed.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128");
    PrepLogicVersions before = PrepLogicVersions.from(original);
    PrepLogicVersions after = PrepLogicVersions.from(changed);
    assertNotEquals(before.version("text"), after.version("text"));
    assertNotEquals(before.version("audio"), after.version("audio"));
    assertEquals(before.version("image"), after.version("image"));
    assertEquals(before.version("video"), after.version("video"));
    changed = new TypedProperties();
    changed.setProperty(EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key(), "2");
    assertNotEquals(before.version("video"), PrepLogicVersions.from(changed).version("video"));
  }

  @Test
  void performanceSettingsDoNotChangeRowsOrTheirVersions() {
    TypedProperties original = new TypedProperties();
    TypedProperties changed = new TypedProperties();
    changed.setProperty(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), "0");
    changed.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "4");
    changed.setProperty(AudioConfigKeys.RUNTIME_PREFIX + "transcriber.whisper.threads", "2");
    changed.setProperty(AudioConfigKeys.RUNTIME_PREFIX + "transcriber.whisper.model.dir", "/models/another-location");
    for (String kind : new String[] {"text", "image", "video", "audio"}) {
      assertEquals(PrepLogicVersions.from(original).version(kind), PrepLogicVersions.from(changed).version(kind));
    }
  }

  @Test
  void persistedPinsRefuseChangedLogicAndMissingKeys() throws Exception {
    TypedProperties props = new TypedProperties();
    UnstructuredTableProperties.Builder settings = UnstructuredTableProperties.builder("Embedding prep")
        .requireTableProperty(UnstructuredTableVersion.BLOB_LAYOUT_VERSION, "1")
        .requireTableProperty(UnstructuredTableVersion.KIND, "embedding")
        .requireTableProperty(UnstructuredTableVersion.SCHEMA_VERSION, "1")
        .requireTableProperty(UnstructuredTableVersion.KEY_ENCODING_VERSION, "1");
    PrepLogicVersions.from(props).requireTableProperties(settings);
    Properties pins = settings.build().tableProperties();
    String base = temp.resolve("table").toUri().toString();
    HoodieTableMetaClient meta = HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(pins)
        .setTableVersion(9).setTableName("embedding").setTableType(HoodieTableType.COPY_ON_WRITE)
        .initTable(HoodieTestUtils.getDefaultStorageConf(), base);
    for (String key : pins.stringPropertyNames()) {
      assertEquals(pins.getProperty(key), meta.getTableConfig().getProps().getProperty(key), key);
    }
    EmbeddingTableChecks.checkTableVersion(base, HoodieTestUtils.getDefaultStorageConf(), EmbeddingTableConfig.from(props));
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128");
    assertThrows(IllegalArgumentException.class, () -> EmbeddingTableChecks.checkTableVersion(base,
        HoodieTestUtils.getDefaultStorageConf(), EmbeddingTableConfig.from(props)));
    HoodieTableConfig missing = new HoodieTableConfig();
    assertThrows(IllegalArgumentException.class, () -> settings.build().checkTable(base, missing));
  }
}
