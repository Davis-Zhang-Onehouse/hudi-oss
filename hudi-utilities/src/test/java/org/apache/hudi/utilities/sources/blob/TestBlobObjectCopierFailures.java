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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Copy failures through copyRow and how each is classified and retried, with path keys, current bytes and unmarked storage. */
class TestBlobObjectCopierFailures {

  private static final String PARTITION = "region=west/pdf";
  private static final String RETRY_ATTEMPTS = "hoodie.blob.table.copy.retry.max.attempts";
  private static final String RETRY_BACKOFF = "hoodie.blob.table.copy.retry.backoff.ms";

  @TempDir
  Path temp;

  private Configuration conf;
  private HoodieStorage storage;
  private OutOfLineWriter writer;
  private String base;

  @BeforeEach
  void setUp() {
    conf = new Configuration();
    BlobUnhappyFileSystem.install(conf, temp.resolve("buckets"), "healthy");
    base = temp.resolve("table").toUri().toString();
    resetStorage();
    ConstructionFailingFileSystem.INITIALIZATIONS.set(0);
  }

  private void resetStorage() {
    storage = spy(new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(conf)));
    writer = new OutOfLineWriter(storage, base, "20261030182900000", Option.empty());
  }

  private String put(byte[] content) throws IOException {
    Path file = BucketFolderFileSystem.file(temp.resolve("buckets"), "bucket", "docs/a.pdf");
    Files.createDirectories(file.getParent());
    Files.write(file, content);
    return BucketFolderFileSystem.SCHEME + "://bucket/docs/a.pdf";
  }

  private static byte[] content() {
    byte[] content = new byte[100];
    for (int i = 0; i < content.length; i++) {
      content[i] = (byte) (i % 251);
    }
    return content;
  }

  private static Row input(String source, long announcedLength) {
    String sha = BlobFileNames.sourcePathSha(source);
    return RowFactory.create(sha, 1234L, source, "a.pdf", "pdf", announcedLength, "application/pdf",
        RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, RowFactory.create(null, 0L, announcedLength, true)));
  }

  private static Properties retry(int attempts) {
    Properties props = new Properties();
    props.setProperty(RETRY_ATTEMPTS, Integer.toString(attempts));
    props.setProperty(RETRY_BACKOFF, "0");
    props.setProperty(BlobTableConfig.COPY_BUFFER_BYTES.key(), "7");
    return props;
  }

  private Row copy(String source, long announcedLength, int attempts) throws IOException {
    return BlobObjectCopier.copyRow(input(source, announcedLength), BlobTableSchema.schema(), retry(attempts), PARTITION,
        storage, writer);
  }

  private List<Path> storedFiles() throws IOException {
    Path root = temp.resolve("table");
    if (!Files.exists(root)) {
      return Collections.emptyList();
    }
    try (Stream<Path> walk = Files.walk(root)) {
      return walk.filter(Files::isRegularFile).filter(path -> path.toString().endsWith(".bin")).collect(Collectors.toList());
    }
  }

  private static void assertIdentity(BlobCopyFailure failure, String source, long announcedLength) {
    assertEquals(BlobFileNames.sourcePathSha(source), failure.getSourcePathSha());
    assertEquals(source, failure.getSourcePath());
    assertEquals(announcedLength, failure.getFileSize());
  }

  @Test
  void confirmedMissingSourceIsAContentFaultWithIdentity() throws Exception {
    String source = "bucketfs://bucket/gone.pdf";
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(source, 17L, 4));
    assertEquals(BlobCopyFailure.Fault.CONTENT, failure.getFault());
    assertTrue(failure.isContentFault());
    assertIdentity(failure, source, 17L);
    assertEquals(1, BlobUnhappyFileSystem.OPENS.get(), "confirmed absence is never retried");
    assertEquals(Collections.emptyList(), storedFiles());
  }

  @ParameterizedTest
  @CsvSource({"open-denied,PLATFORM,1", "access-denied,PLATFORM,1", "open-unknown,TRANSIENT,3"})
  void failedSourceOpenDoesNotConfuseUnreadableAndUnknownAbsence(String mode, BlobCopyFailure.Fault expected, int opens)
      throws Exception {
    String source = put(content());
    conf.set(BlobUnhappyFileSystem.MODE, mode);
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(source, 17L, 3));
    assertEquals(expected, failure.getFault());
    assertFalse(failure.isContentFault());
    assertIdentity(failure, source, 17L);
    assertEquals(opens, BlobUnhappyFileSystem.OPENS.get());
    assertEquals(Collections.emptyList(), storedFiles());
  }

  @Test
  void realPresentUnreadableSourceIsPlatformWithoutRetry() throws Exception {
    Path file = temp.resolve("unreadable.pdf");
    Files.write(file, content());
    Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
    Files.setPosixFilePermissions(file, Collections.emptySet());
    try {
      assumeFalse(Files.isReadable(file), "root can read files with no permission bits");
      String source = file.toUri().toString();
      BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(source, 17L, 3));
      assertEquals(BlobCopyFailure.Fault.PLATFORM, failure.getFault());
      assertIdentity(failure, source, 17L);
      assertEquals(Collections.emptyList(), storedFiles());
      verify(storage, times(1)).create(any(StoragePath.class), eq(false));
    } finally {
      Files.setPosixFilePermissions(file, permissions);
    }
  }

  @Test
  void transientPartialSourceReadRetriesWithActualLengthReset() throws Exception {
    byte[] bytes = content();
    String source = put(bytes);
    conf.set(BlobUnhappyFileSystem.MODE, "partial-once");
    Row result = copy(source, 17L, 3);
    assertEquals(2, BlobUnhappyFileSystem.OPENS.get());
    assertEquals(17L, result.getLong(BlobTableSchema.schema().fieldIndex(BlobTableSchema.SOURCE_FILE_SIZE)),
        "the row keeps the size the status call observed: it is a key column");
    Row blob = result.getStruct(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB));
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, blob.getString(0));
    assertNull(blob.get(1));
    Row reference = blob.getStruct(2);
    assertEquals(0L, reference.getLong(1));
    assertEquals(100L, reference.getLong(2));
    assertTrue(reference.getBoolean(3));
    assertTrue(reference.getString(0).startsWith(".hoodie/.blob/" + PARTITION + "/"));
    Path target = temp.resolve("table").resolve(reference.getString(0));
    assertArrayEquals(bytes, Files.readAllBytes(target));
    assertEquals(Collections.singletonList(target), storedFiles(), "the failed partial copy must be removed");
    for (String unchanged : new String[] {BlobTableSchema.SOURCE_PATH_SHA, BlobTableSchema.SOURCE_MODIFICATION_TIME,
        BlobTableSchema.SOURCE_FILE_PATH, BlobTableSchema.SOURCE_FILE_NAME, BlobTableSchema.SOURCE_FILE_EXTENSION, BlobTableSchema.SOURCE_FILE_SIZE,
        BlobTableSchema.CONTENT_TYPE}) {
      int column = BlobTableSchema.schema().fieldIndex(unchanged);
      assertEquals(input(source, 17L).get(column), result.get(column), unchanged);
    }
  }

  @Test
  void exhaustedTransientRetriesFailTheRoundAndRemoveEveryPartial() throws Exception {
    String source = put(content());
    conf.set(BlobUnhappyFileSystem.MODE, "always-partial");
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(source, 17L, 3));
    assertEquals(BlobCopyFailure.Fault.TRANSIENT, failure.getFault());
    assertEquals("stream", failure.getStep());
    assertIdentity(failure, source, 17L);
    assertEquals(3, BlobUnhappyFileSystem.OPENS.get(), "max attempts includes the initial try");
    assertEquals(Collections.emptyList(), storedFiles());
  }

  @ParameterizedTest
  @CsvSource({"create,missing,TRANSIENT", "write,missing,TRANSIENT", "close,missing,TRANSIENT",
      "create,denied,PLATFORM", "write,denied,PLATFORM", "close,denied,PLATFORM",
      "create,unchecked,TRANSIENT", "write,unchecked,TRANSIENT", "close,unchecked,TRANSIENT"})
  void targetFailuresRetainStageAndDeleteOnlyCreatedPartials(String stage, String kind, BlobCopyFailure.Fault expected)
      throws Exception {
    String source = put(content());
    List<StoragePath> attempted = new ArrayList<>();
    doAnswer(invocation -> {
      attempted.add(invocation.getArgument(0));
      if ("create".equals(stage)) {
        throw targetFailure(kind);
      }
      OutputStream actual = (OutputStream) invocation.callRealMethod();
      return new OutputStream() {
        @Override
        public void write(int value) throws IOException {
          write(new byte[] {(byte) value}, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
          actual.write(bytes, offset, length);
          if ("write".equals(stage)) {
            throw targetFailure(kind);
          }
        }

        @Override
        public void close() throws IOException {
          actual.close();
          if ("close".equals(stage)) {
            throw targetFailure(kind);
          }
        }
      };
    }).when(storage).create(any(StoragePath.class), eq(false));
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(source, 17L, 1));
    assertEquals(expected, failure.getFault(), "a target FileNotFoundException never proves the source absent");
    assertEquals("create".equals(stage) ? "create" : "stream", failure.getStep());
    assertIdentity(failure, source, 17L);
    assertEquals(1, attempted.size());
    assertEquals(Collections.emptyList(), storedFiles());
    assertArrayEquals(content(), Files.readAllBytes(BucketFolderFileSystem.file(temp.resolve("buckets"), "bucket", "docs/a.pdf")));
  }

  private static IOException targetFailure(String kind) {
    if ("denied".equals(kind)) {
      return new AccessDeniedException("target", null, "injected target access denial");
    }
    if ("unchecked".equals(kind)) {
      throw new IllegalStateException("injected unchecked target failure");
    }
    return new FileNotFoundException("injected target upload missing");
  }

  /** Fails source file system construction by mode: an unsupported scheme, an invalid authority or an I/O outage. */
  public static class ConstructionFailingFileSystem extends RawLocalFileSystem {
    static final String MODE = "test.blob.unhappy.construction";
    static final AtomicInteger INITIALIZATIONS = new AtomicInteger();

    @Override
    public void initialize(URI name, Configuration configuration) throws IOException {
      INITIALIZATIONS.incrementAndGet();
      String mode = configuration.get(MODE, "unsupported");
      if ("authority".equals(mode)) {
        throw new IllegalArgumentException("Invalid source authority " + name.getAuthority());
      }
      if ("io".equals(mode)) {
        throw new IOException("injected source initialization outage");
      }
      throw new UnsupportedFileSystemException("No FileSystem for scheme " + name.getScheme());
    }
  }

  /** Leaves the implementation property absent while supplying Hadoop's class lookup. */
  private static class SourceLookupConfiguration extends Configuration {
    private final String property;
    private final Class<?> implementation;
    private int lookups;

    SourceLookupConfiguration(Configuration source, String scheme, Class<?> implementation) {
      super(source);
      this.property = "fs." + scheme + ".impl";
      this.implementation = implementation;
    }

    @Override
    public Class<?> getClass(String name, Class<?> defaultValue) {
      if (!name.equals(property)) {
        return super.getClass(name, defaultValue);
      }
      lookups++;
      if (implementation == null) {
        throw new RuntimeException(new ClassNotFoundException("injected missing connector class"));
      }
      return implementation;
    }
  }

  private void assertConstructionFault(String source, BlobCopyFailure.Fault expected, int initializations) throws Exception {
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(source, 17L, 3));
    assertEquals(expected, failure.getFault());
    assertIdentity(failure, source, 17L);
    assertEquals(initializations, ConstructionFailingFileSystem.INITIALIZATIONS.get());
    assertEquals(Collections.emptyList(), storedFiles());
  }

  @Test
  void unknownSchemeIsContentWithoutRetry() throws Exception {
    conf.unset("fs.unservedcopyfs.impl");
    assertConstructionFault("unservedcopyfs://bucket/docs/a.pdf", BlobCopyFailure.Fault.CONTENT, 0);
  }

  @Test
  void missingConnectorOfConfiguredSchemeIsPlatformWithoutRetry() throws Exception {
    conf.set("fs.copyserved.impl", ConstructionFailingFileSystem.class.getName());
    conf.setBoolean("fs.copyserved.impl.disable.cache", true);
    assertConstructionFault("copyserved://bucket/docs/a.pdf", BlobCopyFailure.Fault.PLATFORM, 1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void missingConnectorOfKnownSchemeIsPlatformWithoutRetry(boolean missingClass) throws Exception {
    SourceLookupConfiguration lookup = new SourceLookupConfiguration(conf, "s3a",
        missingClass ? null : ConstructionFailingFileSystem.class);
    lookup.unset("fs.s3a.impl");
    lookup.setBoolean("fs.s3a.impl.disable.cache", true);
    conf = lookup;
    resetStorage();
    assertConstructionFault("s3a://bucket/docs/a.pdf", BlobCopyFailure.Fault.PLATFORM, missingClass ? 0 : 1);
    assertEquals(1, lookup.lookups);
    assertNull(conf.get("fs.s3a.impl"));
  }

  @Test
  void malformedPathIsContentWithoutRetry() throws Exception {
    assertConstructionFault("s3a://[invalid/docs/a.pdf", BlobCopyFailure.Fault.CONTENT, 0);
  }

  @Test
  void malformedAuthorityIsContentWithoutRetry() throws Exception {
    conf.set("fs.s3a.impl", ConstructionFailingFileSystem.class.getName());
    conf.setBoolean("fs.s3a.impl.disable.cache", true);
    conf.set(ConstructionFailingFileSystem.MODE, "authority");
    assertConstructionFault("s3a://bad_bucket/docs/a.pdf", BlobCopyFailure.Fault.CONTENT, 1);
  }

  @Test
  void validSourceConstructionIoIsTransientAndRetried() throws Exception {
    conf.set("fs.copyserved.impl", ConstructionFailingFileSystem.class.getName());
    conf.setBoolean("fs.copyserved.impl.disable.cache", true);
    conf.set(ConstructionFailingFileSystem.MODE, "io");
    assertConstructionFault("copyserved://bucket/docs/a.pdf", BlobCopyFailure.Fault.TRANSIENT, 3);
  }
}
