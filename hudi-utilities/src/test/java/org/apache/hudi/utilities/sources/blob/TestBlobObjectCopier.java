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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Copies stream the current bytes to a fresh managed path; inline and unmanaged values are kept. */
class TestBlobObjectCopier {
  @TempDir
  Path temp;

  private static Row row(String source, long announcedLength, Row blob) {
    String sha = BlobFileNames.sourcePathSha(source);
    return RowFactory.create(sha, 1234L, source, "a.pdf", "pdf", announcedLength, "application/pdf", blob);
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

}
