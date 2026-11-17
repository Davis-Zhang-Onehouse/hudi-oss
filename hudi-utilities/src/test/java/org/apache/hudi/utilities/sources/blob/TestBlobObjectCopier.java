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
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Raw blob copies: managed copies stream the current bytes to EOF, inline and unmanaged values pass unchanged, copy settings are bounded. */
class TestBlobObjectCopier {
  @TempDir
  Path temp;

  private static Row row(String source, long announcedLength, Row blob) {
    String sha = BlobFileNames.sourcePathSha(source);
    return RowFactory.create(sha, 1234L, source, "a.pdf", "pdf", announcedLength, "application/pdf", blob);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 131073})
  void managedCopyStreamsToEofAndKeepsTheStatusSizeInTheRow(int length) throws Exception {
    byte[] bytes = new byte[length];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (i % 251);
    }
    Path source = temp.resolve("a.pdf");
    Files.write(source, bytes);
    String base = temp.resolve("table").toUri().toString();
    HoodieHadoopStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()));
    Properties props = new Properties();
    props.setProperty(BlobTableConfig.COPY_BUFFER_BYTES.key(), "7");
    Row input = row(source.toUri().toString(), 17L, RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null,
        RowFactory.create("", 0L, 17L, true)));
    Row result = BlobObjectCopier.copyRow(input, BlobTableSchema.schema(), props, "region=west/pdf", storage,
        new OutOfLineWriter(storage, base, "101", Option.empty()));
    assertEquals(17L, result.getLong(BlobTableSchema.schema().fieldIndex(BlobTableSchema.SOURCE_FILE_SIZE)),
        "the row keeps the status call's size (a key column); the reference carries what was copied");
    Row blob = result.getStruct(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB));
    assertNull(blob.get(1));
    Row reference = blob.getStruct(2);
    assertTrue(reference.getString(0).startsWith(".hoodie/.blob/region=west/pdf/"));
    assertEquals(0L, reference.getLong(1));
    assertEquals((long) length, reference.getLong(2));
    assertTrue(reference.getBoolean(3));
    assertArrayEquals(bytes, Files.readAllBytes(temp.resolve("table").resolve(reference.getString(0))));
    for (String unchanged : new String[] {BlobTableSchema.SOURCE_PATH_SHA, BlobTableSchema.SOURCE_MODIFICATION_TIME,
        BlobTableSchema.SOURCE_FILE_PATH, BlobTableSchema.SOURCE_FILE_NAME, BlobTableSchema.SOURCE_FILE_EXTENSION, BlobTableSchema.SOURCE_FILE_SIZE,
        BlobTableSchema.CONTENT_TYPE}) {
      int column = BlobTableSchema.schema().fieldIndex(unchanged);
      assertEquals(input.get(column), result.get(column), unchanged);
    }
  }

  @Test
  void inlineAndUnmanagedValuesAreUnchanged() throws Exception {
    for (Row blob : new Row[] {RowFactory.create(HoodieSchema.Blob.INLINE, new byte[] {1}, null),
        RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, RowFactory.create("s3a://foreign/a.pdf", 2L, 3L, false))}) {
      Row input = row("file:///a.pdf", 1L, blob);
      assertSame(input, BlobObjectCopier.copyRow(input, BlobTableSchema.schema(), new Properties(), "pdf", null, null));
    }
  }

  @Test
  void nullBlobIsAContentFaultWithIdentity() {
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class,
        () -> BlobObjectCopier.copyRow(row("file:///a.pdf", 17L, null), BlobTableSchema.schema(), new Properties(), "pdf", null, null));
    assertTrue(failure.isContentFault());
    assertEquals(BlobFileNames.sourcePathSha("file:///a.pdf"), failure.getSourcePathSha());
    assertTrue(failure.getMessage().contains("gone since announced"), failure.getMessage());
    assertEquals("file:///a.pdf", failure.getSourcePath());
    assertEquals(17L, failure.getFileSize());
    assertEquals("read", failure.getStep());
  }

  @Test
  void outOfLineBlobWithoutReferenceIsAContentFault() {
    Row blob = RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, null);
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class,
        () -> BlobObjectCopier.copyRow(row("file:///a.pdf", 17L, blob), BlobTableSchema.schema(), new Properties(), "pdf", null, null));
    assertTrue(failure.isContentFault());
    assertEquals("file:///a.pdf", failure.getSourcePath());
    assertEquals("read", failure.getStep());
  }

  @Test
  void outOfLineBlobWithoutManagedFlagIsAContentFault() {
    Row blob = RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, RowFactory.create("file:///a.pdf", 0L, 17L, null));
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class,
        () -> BlobObjectCopier.copyRow(row("file:///a.pdf", 17L, blob), BlobTableSchema.schema(), new Properties(), "pdf", null, null));
    assertTrue(failure.isContentFault());
    assertEquals("read", failure.getStep());
  }

  @Test
  void copyBufferIsBoundedAndInvalidSettingsNameTheKey() {
    assertEquals(65536, BlobTableConfig.copyBufferBytes(new Properties()));
    for (String raw : new String[] {"0", "-1", "invalid"}) {
      Properties props = new Properties();
      props.setProperty(BlobTableConfig.COPY_BUFFER_BYTES.key(), raw);
      IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> BlobTableConfig.copyBufferBytes(props));
      assertTrue(failure.getMessage().contains(BlobTableConfig.COPY_BUFFER_BYTES.key()));
    }
  }
}
