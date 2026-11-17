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
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Streams current source bytes into fresh managed files; inline and unmanaged values retain their content. */
public final class BlobObjectCopier {

  public static final String STEP_READ = "read";

  private BlobObjectCopier() {
  }

  /**
   * Copies a managed reference into a fresh out-of-line file and returns a row carrying that relative path. Inline bytes and
   * unmanaged references retain their content. The partition comes from the same configured key generator as the write. The row
   * keeps the size the status call observed (half of its version tag, a key column); the reference's length is the bytes the copy
   * streamed to EOF, which differ when the object changed between the status call and the copy.
   */
  static Row copyRow(Row row, StructType schema, Properties props, String partition, HoodieStorage storage,
                     OutOfLineWriter writer) throws IOException {
    String sourcePath = row.getString(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_PATH));
    long fileSize = row.getLong(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_SIZE));
    String sourcePathSha = row.getString(schema.fieldIndex(BlobTableSchema.SOURCE_PATH_SHA));
    int blobIndex = schema.fieldIndex(BlobTableSchema.BLOB);
    if (row.isNullAt(blobIndex)) {
      // a row with no blob has no bytes to copy: a content fault of the record
      throw new BlobCopyFailure(sourcePathSha, sourcePath, fileSize, STEP_READ,
          "gone since announced: no bytes were read when the row was prepared");
    }
    Row blob = row.getStruct(blobIndex);
    StructType blobType = (StructType) schema.fields()[blobIndex].dataType();
    int referenceIndex = blobType.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE);
    StructType referenceType = (StructType) blobType.fields()[referenceIndex].dataType();
    if (!HoodieSchema.Blob.OUT_OF_LINE.equals(blob.getString(blobType.fieldIndex(HoodieSchema.Blob.TYPE)))) {
      return row;
    }
    if (blob.isNullAt(referenceIndex)) {
      throw new BlobCopyFailure(sourcePathSha, sourcePath, fileSize, STEP_READ, "out-of-line blob without a reference");
    }
    Row reference = blob.getStruct(referenceIndex);
    int managedIndex = referenceType.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED);
    if (reference.isNullAt(managedIndex)) {
      throw new BlobCopyFailure(sourcePathSha, sourcePath, fileSize, STEP_READ, "out-of-line blob reference without a managed flag");
    }
    if (!reference.getBoolean(managedIndex)) {
      return row;
    }
    Copy copy = new Copy(partition, sourcePath);
    int bufferBytes = BlobTableConfig.copyBufferBytes(props);
    String externalPath = copy(storage, copy, bufferBytes, writer);
    Object[] values = new Object[row.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = row.get(i);
    }
    values[blobIndex] = RowFactory.create(blob.get(0), blob.get(1),
        RowFactory.create(externalPath, 0L, copy.written, true));
    return RowFactory.create(values);
  }

  /** One streaming copy, recording the actual length through EOF rather than trusting notification metadata. */
  static final class Copy {
    final String sourcePath;
    final String partition;
    long written;

    Copy(String partition, String sourcePath) {
      this.sourcePath = sourcePath;
      this.partition = partition;
    }
  }

  static String copy(HoodieStorage storage, Copy copy, int bufferBytes, OutOfLineWriter writer) throws IOException {
    Configuration conf = storage.getConf().unwrapAs(Configuration.class);
    Path source = new Path(copy.sourcePath);
    FileSystem sourceFs = HadoopFSUtils.getFs(source, conf);
    return writer.write(copy.partition, out -> {
      try (InputStream in = sourceFs.open(source)) {
        byte[] buffer = new byte[bufferBytes];
        int n;
        while ((n = in.read(buffer)) >= 0) {
          out.write(buffer, 0, n);
          copy.written += n;
        }
      }
    });
  }

}
