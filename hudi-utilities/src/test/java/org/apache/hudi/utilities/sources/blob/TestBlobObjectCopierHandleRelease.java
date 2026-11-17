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
import org.apache.hudi.utilities.testutils.HandleCountingFileSystem;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The blob copy: {@link BlobObjectCopier#copyRow} closes the source stream and the target stream of every attempt, and a failed
 * attempt leaves no file under the blob folder (the shared writer removes it), on the main path, a retried copy and a failed one.
 */
public class TestBlobObjectCopierHandleRelease {

  @TempDir
  Path tempDir;

  private Path source;
  private Path table;
  private byte[] content;

  @BeforeEach
  public void writeSource() throws IOException {
    HandleCountingFileSystem.reset();
    source = tempDir.resolve("source").resolve("a.pdf");
    table = tempDir.resolve("table");
    Files.createDirectories(source.getParent());
    Files.createDirectories(table);
    content = new byte[1000];
    for (int i = 0; i < content.length; i++) {
      content[i] = (byte) (i % 251);
    }
    Files.write(source, content);
  }

  @AfterEach
  public void resetRegistry() {
    HandleCountingFileSystem.reset();
  }

  @Test
  public void mainPathClosesTheSourceAndTargetStreams() throws IOException {
    Row copied = copy(props(1));
    assertEquals(2, HandleCountingFileSystem.opened(), "one source read and one target write");
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by the main copy");
    String externalPath = copied.getStruct(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB)).getStruct(2).getString(0);
    assertEquals(Collections.singletonList(externalPath), blobFiles());
    assertArrayEquals(content, Files.readAllBytes(table.resolve(externalPath)));
  }

  @Test
  public void retriedCopyClosesEveryAttemptAndKeepsOnlyTheLastFile() throws IOException {
    // the first attempt's first source read fails transiently; the second attempt copies
    HandleCountingFileSystem.inject(source, HandleCountingFileSystem.Fault.READ, 1);
    Row copied = copy(props(3));
    assertEquals(4, HandleCountingFileSystem.opened(), "two attempts, each with a source read and a target write");
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by a retried copy");
    String externalPath = copied.getStruct(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB)).getStruct(2).getString(0);
    assertEquals(Collections.singletonList(externalPath), blobFiles(), "the failed attempt's file is removed");
  }

  @Test
  public void failedCopyClosesBothStreamsAndLeavesNoFile() throws IOException {
    HandleCountingFileSystem.inject(source, HandleCountingFileSystem.Fault.READ, 1);
    assertThrows(BlobCopyFailure.class, () -> copy(props(1)));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by a failed copy");
    assertEquals(Collections.emptyList(), blobFiles(), "files left by a failed copy");
  }

  @Test
  public void failedTargetCloseClosesTheSourceAndLeavesNoFile() throws IOException {
    HandleCountingFileSystem.inject(table, HandleCountingFileSystem.Fault.CLOSE_WRITE, 1);
    assertThrows(BlobCopyFailure.class, () -> copy(props(1)));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "streams left open by a failed target close");
    assertEquals(Collections.emptyList(), blobFiles(), "files left by a failed target close");
  }

  private Row copy(Properties props) throws IOException {
    Configuration conf = HandleCountingFileSystem.install(new Configuration());
    String base = HandleCountingFileSystem.url(table);
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(conf));
    String sourceUrl = HandleCountingFileSystem.url(source);
    Row input = RowFactory.create(BlobFileNames.sourcePathSha(sourceUrl), 1234L, sourceUrl, "a.pdf", "pdf", (long) content.length,
        "application/pdf", RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, RowFactory.create(null, 0L, (long) content.length, true)));
    return BlobObjectCopier.copyRow(input, BlobTableSchema.schema(), props, "pdf", storage,
        new OutOfLineWriter(storage, base, "101", Option.empty()));
  }

  private static Properties props(int attempts) {
    Properties props = new Properties();
    props.setProperty(BlobTableConfig.COPY_BUFFER_BYTES.key(), "64");
    props.setProperty(BlobTableConfig.COPY_RETRY_MAX_ATTEMPTS.key(), String.valueOf(attempts));
    props.setProperty(BlobTableConfig.COPY_RETRY_BACKOFF_MS.key(), "0");
    return props;
  }

  /** Every file under the table's blob folder, relative to the table. */
  private List<String> blobFiles() throws IOException {
    Path blobFolder = table.resolve(".hoodie").resolve(".blob");
    if (!Files.exists(blobFolder)) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.walk(blobFolder)) {
      return files.filter(Files::isRegularFile).map(file -> table.relativize(file).toString()).sorted().collect(Collectors.toList());
    }
  }
}
