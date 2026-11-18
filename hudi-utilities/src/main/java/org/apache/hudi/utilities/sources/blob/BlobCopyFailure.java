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

import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.nio.file.FileSystemException;

/**
 * Identifies a blob content fault for ordinary per-row transformer error routing. Storage faults fail the round and leave
 * its checkpoint unchanged; completed unmarked files can remain unreferenced.
 */
public class BlobCopyFailure extends HoodieIOException {

  private static final long serialVersionUID = 1L;

  private final String sourcePathSha;
  private final String sourcePath;
  private final long fileSize;
  private final String step;
  private final Fault fault;

  /** What a copy failure says about its record. */
  public enum Fault {
    /** The record's own content: quarantined, the round commits. */
    CONTENT,
    /** A transient storage fault: not the record's fault, so the round fails and its checkpoint is unchanged. */
    TRANSIENT,
    /** A platform fault such as an access denial: the round fails at once. */
    PLATFORM
  }

  /** A content fault of the record, described by {@code cause}. */
  public BlobCopyFailure(String sourcePathSha, String sourcePath, long fileSize, String step, String cause) {
    super(message(sourcePathSha, sourcePath, fileSize, step, cause));
    this.sourcePathSha = sourcePathSha;
    this.sourcePath = sourcePath;
    this.fileSize = fileSize;
    this.step = step;
    this.fault = Fault.CONTENT;
  }

  /** A failure caused by {@code cause}, of the given {@code fault}. */
  public BlobCopyFailure(String sourcePathSha, String sourcePath, long fileSize, String step, IOException cause, Fault fault) {
    super(message(sourcePathSha, sourcePath, fileSize, step, cause.getClass().getSimpleName() + ": " + cause.getMessage()), portable(cause));
    this.sourcePathSha = sourcePathSha;
    this.sourcePath = sourcePath;
    this.fileSize = fileSize;
    this.step = step;
    this.fault = fault;
  }

  /**
   * {@code cause}, or, when its chain holds a {@link FileSystemException} (S3A reports a 403 as {@code AccessDeniedException}), an
   * {@link IOException} naming each throwable of the chain with the original stack: the failed record's exception travels in its
   * Spark job failure, which Spark's Kryo serializer cannot serialize with a {@code java.nio.file} exception inside on Java 17.
   */
  static IOException portable(IOException cause) {
    boolean fileSystemException = false;
    StringBuilder chain = new StringBuilder();
    for (Throwable t : ExceptionUtil.getCauses(cause)) {
      fileSystemException |= t instanceof FileSystemException;
      chain.append(chain.length() == 0 ? "" : " <- ").append(t.getClass().getName()).append(": ").append(t.getMessage());
    }
    if (!fileSystemException) {
      return cause;
    }
    IOException copy = new IOException(chain.toString());
    copy.setStackTrace(cause.getStackTrace());
    return copy;
  }

  private static String message(String sourcePathSha, String sourcePath, long fileSize, String step, String cause) {
    return step + ": source_path_sha " + sourcePathSha + " " + sourcePath + " " + fileSize + ": " + cause;
  }

  public String getSourcePathSha() {
    return sourcePathSha;
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public long getFileSize() {
    return fileSize;
  }

  public String getStep() {
    return step;
  }

  public Fault getFault() {
    return fault;
  }

  public boolean isContentFault() {
    return fault == Fault.CONTENT;
  }
}
