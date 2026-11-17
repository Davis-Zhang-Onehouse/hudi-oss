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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Builds blob metadata rows and bounded inline values from the current object size. Storage read failures propagate as {@link HoodieException}.
 */
public class BlobRowBuilder implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StorageConfiguration<?> storageConf;
  private final long inlineMaxBytes;
  private final double inlineMaxHeapFraction;
  private final boolean inlineCeilingEnforced;
  private transient boolean taskCeilingChecked;

  public BlobRowBuilder(TypedProperties props, StorageConfiguration<?> storageConf) {
    this.storageConf = storageConf;
    this.inlineMaxBytes = BlobTableConfig.inlineMaxBytes(props);
    this.inlineMaxHeapFraction = BlobInlineCeiling.fraction(props);
    this.inlineCeilingEnforced = ConfigUtils.getBooleanWithAltKeys(props, BlobTableConfig.INLINE_CEILING_ENFORCE);
  }

  /** Lazily maps one Spark partition of selected objects to rows of {@link BlobTableSchema#schema()}. */
  public Iterator<Row> build(Iterator<CloudObjectMetadata> files) {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return files.hasNext();
      }

      @Override
      public Row next() {
        return buildRow(files.next());
      }
    };
  }

  /**
   * Builds one row of one version of the object; reads the bytes of an inline-size object, touches nothing under the table. The
   * version tag (modification time, size) is the object's as its status reports it (a storage that reports no modification time
   * gives 0, and its versions then differ by size alone). An object that is confirmed gone is a row with no blob, which a later
   * stage quarantines as gone since announced; its version tag is what could be observed: gone before the status call, no
   * modification time (0) and the announced size; gone between the status call and the read, the status call's tag.
   */
  Row buildRow(CloudObjectMetadata file) {
    // the object's own path, not normalized
    String sourcePath = file.getPath();
    String fileName = BlobFileNames.fileName(sourcePath);
    String extension = BlobFileNames.extension(fileName);
    String sourcePathSha = BlobFileNames.sourcePathSha(sourcePath);
    String contentType = BlobFileNames.contentType(extension);
    StoragePath path = new StoragePath(sourcePath);
    // the announced size and no observed modification time: what a row keeps when the object is gone before the status call
    long currentSize = file.getSize();
    long modificationTime = 0L;
    Row blob;
    try {
      // one status call per object: its length and modification time (S3: LastModified, ms) are the row's version tag
      StoragePathInfo info = HoodieStorageUtils.getStorage(path, storageConf).getPathInfo(path);
      currentSize = info.getLength();
      modificationTime = info.getModificationTime();
      if (currentSize <= inlineMaxBytes) {
        checkTaskCeiling();
        byte[] bytes = readInline(path, currentSize);
        blob = RowFactory.create(HoodieSchema.Blob.INLINE, bytes, null);
      } else {
        // The copy transformer fills the required path after streaming the current source bytes.
        blob = RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null,
            RowFactory.create(null, 0L, currentSize, true));
      }
    } catch (IOException e) {
      if (!isConfirmedMissing(path, e)) {
        throw new HoodieIOException("Failed to read source file " + sourcePath, e);
      }
      // A row with no blob: the copy transformer and the embedding transformer each quarantine it as gone since announced.
      blob = null;
    }
    return RowFactory.create(sourcePathSha, modificationTime, sourcePath, fileName, extension, currentSize, contentType, blob);
  }

  /**
   * Checks the inline threshold against this task's {@link BlobInlineCeiling} (the executor's own max heap) once per builder
   * instance, before its first inline read. Outside a Spark task there is no task heap share to check against.
   */
  private void checkTaskCeiling() {
    if (taskCeilingChecked) {
      return;
    }
    if (TaskContext.get() != null) {
      BlobInlineCeiling.inTask(SparkEnv.get().conf(), inlineMaxHeapFraction)
          .check(inlineMaxBytes, inlineCeilingEnforced, "in the task before its first inline read");
    }
    taskCeilingChecked = true;
  }

  private boolean isConfirmedMissing(StoragePath path, IOException failure) {
    if (BlobObjectCopier.isCausedByAccessDenial(failure)
        || !ExceptionUtil.isCausedBy(failure, FileNotFoundException.class)) {
      return false;
    }
    try {
      return !HoodieStorageUtils.getStorage(path, storageConf).exists(path);
    } catch (IOException | RuntimeException recheckFailure) {
      return false;
    }
  }

  private byte[] readInline(StoragePath path, long size) throws IOException {
    byte[] bytes = new byte[(int) size];
    try (DataInputStream in = new DataInputStream(HoodieStorageUtils.getStorage(path, storageConf).open(path))) {
      in.readFully(bytes);
      // an object overwritten with a larger one after the status call: its first bytes are not the version the row is tagged with
      if (in.read() != -1) {
        throw new IOException("Source file " + path + " holds more than the " + size + " bytes its status call reported: "
            + "it changed after the status call");
      }
    }
    return bytes;
  }
}
