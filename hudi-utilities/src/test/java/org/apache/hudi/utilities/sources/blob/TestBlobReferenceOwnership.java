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
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Layout-one ownership is carried by path form at the copy writer boundary. */
class TestBlobReferenceOwnership {
  private static final StructType SCHEMA = BlobTableSchema.schema();
  private static final int BLOB_INDEX = SCHEMA.fieldIndex(BlobTableSchema.BLOB);
  private static final StructType BLOB_TYPE = (StructType) SCHEMA.fields()[BLOB_INDEX].dataType();

  @TempDir
  Path temp;

  @ParameterizedTest
  @ValueSource(strings = {".hoodie/.blob/pdf/a.bin", "/.hoodie/.blob/pdf/a.bin"})
  void refusesSchemeLessUserManagedReferencesBeforeWriting(String externalPath) throws Exception {
    Row input = row(externalPath, false);
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(input));
    assertTrue(failure.isContentFault());
    assertTrue(failure.getMessage().contains("managed=true"), failure.getMessage());
    assertTrue(failure.getMessage().contains(externalPath), failure.getMessage());
    assertFalse(Files.exists(temp.resolve("table/.hoodie/.blob")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"file:///user/a.pdf", "s3a://bucket/a.pdf"})
  void refusesAbsoluteHudiManagedReferencesBeforeCopying(String externalPath) throws Exception {
    Row input = row(externalPath, true);
    BlobCopyFailure failure = assertThrows(BlobCopyFailure.class, () -> copy(input));
    assertTrue(failure.isContentFault());
    assertTrue(failure.getMessage().contains("managed=false"), failure.getMessage());
    assertTrue(failure.getMessage().contains(externalPath), failure.getMessage());
    assertFalse(Files.exists(temp.resolve("table/.hoodie/.blob")));
  }

  @Test
  void managedCopyPersistsARelativeReferenceReadableUnderTheTableBase() throws Exception {
    Row output = copy(row(null, true));
    Row blob = output.getStruct(BLOB_INDEX);
    Row reference = blob.getStruct(2);
    String relative = reference.getString(0);
    assertTrue(relative.startsWith(".hoodie/.blob/pdf/"), relative);
    assertNull(new StoragePath(relative).toUri().getScheme());
    assertTrue(reference.getBoolean(3));
    assertArrayEquals(new byte[] {3, 1, 4, 1}, read(blob));
  }

  @Test
  void absoluteUserReferenceIsPreservedAndReadAsGiven() throws Exception {
    Path external = temp.resolve("external.bin");
    Files.write(external, new byte[] {9, 2, 6, 5});
    String absolute = external.toUri().toString();
    Row input = row(absolute, false);
    Row output = copy(input);
    assertSame(input, output);
    Row blob = output.getStruct(BLOB_INDEX);
    assertEquals(absolute, blob.getStruct(2).getString(0));
    assertFalse(blob.getStruct(2).getBoolean(3));
    assertArrayEquals(new byte[] {9, 2, 6, 5}, read(blob));
    assertFalse(Files.exists(temp.resolve("table/.hoodie/.blob")));
  }

  private Row row(String externalPath, boolean managed) throws Exception {
    Path source = temp.resolve("source.pdf");
    Files.write(source, new byte[] {3, 1, 4, 1});
    String uri = source.toUri().toString();
    return RowFactory.create(BlobFileNames.sourcePathSha(uri), 1234L, uri, "source.pdf", "pdf", 4L, "application/pdf",
        RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, RowFactory.create(externalPath, 0L, 4L, managed)));
  }

  private Row copy(Row input) throws Exception {
    String base = temp.resolve("table").toUri().toString();
    try (HoodieHadoopStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()))) {
      return BlobObjectCopier.copyRow(input, SCHEMA, new Properties(), "pdf", storage,
          new OutOfLineWriter(storage, base, "101", Option.empty()));
    }
  }

  private byte[] read(Row blob) throws Exception {
    StructType referenceType = (StructType) BLOB_TYPE.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
    Row reference = blob.getStruct(2);
    Row namedReference = new GenericRowWithSchema(new Object[] {
        reference.get(0), reference.get(1), reference.get(2), reference.get(3)}, referenceType);
    Row namedBlob = new GenericRowWithSchema(new Object[] {blob.get(0), blob.get(1), namedReference}, BLOB_TYPE);
    BlobBytesSource reader = new BlobBytesSource(temp.resolve("table").toUri().toString(),
        HadoopFSUtils.getStorageConf(new Configuration()));
    try (InputStream in = reader.open(namedBlob)) {
      return FileIOUtils.readAsByteArray(in);
    }
  }
}
