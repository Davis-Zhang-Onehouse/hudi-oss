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
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;
import org.apache.hudi.utilities.unstructured.storage.StorageReferenceRules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.Properties;

/** Streams current source bytes into fresh managed files; inline and unmanaged values retain their content. */
public final class BlobObjectCopier {

  /** The simple name of the HTTP response exception the GCS connector's refused requests are caused by. */
  private static final String GCS_HTTP_RESPONSE_EXCEPTION = "HttpResponseException";
  /** The HTTP status of a refused request. */
  private static final int HTTP_FORBIDDEN = 403;
  /**
   * The reasons of a GCS 403 that refuses a request for its rate, not its access: the JSON error names one of them
   * ({@code "reason": "rateLimitExceeded"}), and a later attempt may pass.
   */
  private static final String[] GCS_RATE_LIMIT_REASONS = {"\"rateLimitExceeded\"", "\"userRateLimitExceeded\""};

  public static final String STEP_READ = "read";
  public static final String STEP_CREATE = "create";
  public static final String STEP_STREAM = "stream";

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
      // the source confirmed the object absent at its status call or its inline read: the row carries no bytes
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
    boolean managed = reference.getBoolean(managedIndex);
    int pathIndex = referenceType.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH);
    String suppliedPath = reference.isNullAt(pathIndex) ? null : reference.getString(pathIndex);
    // The source builder's pending managed copy has no path until its bytes have been copied.
    if (!managed || (suppliedPath != null && !suppliedPath.isEmpty())) {
      try {
        StorageReferenceRules.validateWriteOwnership(suppliedPath, managed);
      } catch (IllegalArgumentException invalidReference) {
        throw new BlobCopyFailure(sourcePathSha, sourcePath, fileSize, STEP_READ, invalidReference.getMessage());
      }
    }
    if (!managed) {
      return row;
    }
    Copy copy = new Copy(sourcePathSha, sourcePath, fileSize, partition);
    int bufferBytes = BlobTableConfig.copyBufferBytes(props);
    String externalPath = BlobTableConfig.copyRetry(props).call("blob copy of source_path_sha " + sourcePathSha,
        () -> copy(storage, copy, bufferBytes, writer), BlobObjectCopier::isTransientCopyFault);
    StorageReferenceRules.validateWriteOwnership(externalPath, true);
    Object[] values = new Object[row.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = row.get(i);
    }
    values[blobIndex] = RowFactory.create(blob.get(0), blob.get(1),
        RowFactory.create(externalPath, 0L, copy.written, true));
    return RowFactory.create(values);
  }

  /** Whether the copy failure permits another in-round attempt. */
  static boolean isTransientCopyFault(Throwable failure) {
    return failure instanceof BlobCopyFailure && ((BlobCopyFailure) failure).getFault() == BlobCopyFailure.Fault.TRANSIENT;
  }

  /** One streaming copy, recording its actual EOF length independently for every attempt. */
  static final class Copy {
    final String sourcePathSha;
    final String sourcePath;
    final long fileSize;
    final String partition;
    long written;

    Copy(String sourcePathSha, String sourcePath, long fileSize, String partition) {
      this.sourcePathSha = sourcePathSha;
      this.sourcePath = sourcePath;
      this.fileSize = fileSize;
      this.partition = partition;
    }

    /**
     * A failure caused by {@code cause} while accessing the source ({@code sourceFs} not null, {@code source}) or the target
     * ({@code sourceFs} null): an access denial ({@link #isAccessDenial}) is a platform fault; a
     * FileNotFoundException of a source access is the record's content fault only when a re-check finds the source absent, and an
     * access denial when the source is there (a local "Permission denied"); any other storage failure, a
     * FileNotFoundException of the target included (e.g. an aborted multipart upload), is transient.
     */
    BlobCopyFailure failure(String step, IOException cause, FileSystem sourceFs, Path source) {
      if (isCausedByAccessDenial(cause)) {
        return new BlobCopyFailure(sourcePathSha, sourcePath, fileSize, step, cause, BlobCopyFailure.Fault.PLATFORM);
      }
      if (sourceFs != null && ExceptionUtil.isCausedBy(cause, FileNotFoundException.class)) {
        Option<Boolean> exists = sourceExists(sourceFs, source);
        if (exists.isPresent()) {
          // absent: the record's content; there but cannot be read (a local "Permission denied"): an access denial
          BlobCopyFailure.Fault fault = Boolean.TRUE.equals(exists.get()) ? BlobCopyFailure.Fault.PLATFORM : BlobCopyFailure.Fault.CONTENT;
          return new BlobCopyFailure(sourcePathSha, sourcePath, fileSize, step, cause, fault);
        }
      }
      return new BlobCopyFailure(sourcePathSha, sourcePath, fileSize, step, cause, BlobCopyFailure.Fault.TRANSIENT);
    }
  }

  /** Whether {@code failure} or any of its causes is an access denial ({@link #isAccessDenial}). */
  static boolean isCausedByAccessDenial(Throwable failure) {
    for (Throwable cause : ExceptionUtil.getCauses(failure)) {
      if (isAccessDenial(cause)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether {@code cause} itself is an access denial: {@link AccessDeniedException} (how S3A reports a 403 or a KMS denial),
   * Hadoop's {@link AccessControlException} (an HDFS permission check), or the GCS connector's HTTP 403 response unless it
   * names a rate-limit reason ({@code rateLimitExceeded}, {@code userRateLimitExceeded}): such a 403 is a quota a later attempt
   * may pass, so it takes the retry path. The GCS connector raises an IOException caused by its JSON error response, whose
   * class is relocated under the connector's own package in the shaded connector jar, so it is recognized by its class's
   * simple name in the hierarchy ({@code HttpResponseException}) and its message, which starts with the status code and
   * carries the JSON error with its reason.
   */
  public static boolean isAccessDenial(Throwable cause) {
    if (cause instanceof AccessDeniedException || cause instanceof AccessControlException) {
      return true;
    }
    for (Class<?> type = cause.getClass(); type != null; type = type.getSuperclass()) {
      if (GCS_HTTP_RESPONSE_EXCEPTION.equals(type.getSimpleName())) {
        String message = cause.getMessage();
        return message != null && message.startsWith(HTTP_FORBIDDEN + " ") && !namesRateLimit(message);
      }
    }
    return false;
  }

  private static boolean namesRateLimit(String message) {
    for (String reason : GCS_RATE_LIMIT_REASONS) {
      if (message.contains(reason)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether {@code fs} answers that {@code path} exists; empty when it cannot be asked (no answer is no proof of absence). A
   * FileNotFoundException of a source is its absence only when this answers false: it also reports a file that is there but
   * may not be read (a local "Permission denied").
   */
  static Option<Boolean> sourceExists(FileSystem fs, Path path) {
    try {
      return Option.of(fs.exists(path));
    } catch (IOException | RuntimeException e) {
      return Option.empty();
    }
  }

  static String copy(HoodieStorage storage, Copy copy, int bufferBytes, OutOfLineWriter writer) throws IOException {
    copy.written = 0L;
    Configuration conf = storage.getConf().unwrapAs(Configuration.class);
    Path source = null;
    FileSystem sourceFs;
    try {
      source = new Path(copy.sourcePath);
      sourceFs = HadoopFSUtils.getFs(source, conf);
    } catch (RuntimeException e) {
      BlobCopyFailure.Fault fault = source == null ? BlobCopyFailure.Fault.CONTENT : sourceLookupFault(source, conf, e);
      throw new BlobCopyFailure(copy.sourcePathSha, copy.sourcePath, copy.fileSize, STEP_READ, new IOException(e.getMessage(), e), fault);
    }
    Path sourcePath = source;
    String[] step = {STEP_CREATE};
    boolean[] onSource = {false};
    try {
      return writer.write(copy.partition, out -> {
        step[0] = STEP_STREAM;
        onSource[0] = true;
        try (InputStream in = sourceFs.open(sourcePath)) {
          byte[] buffer = new byte[bufferBytes];
          while (true) {
            onSource[0] = true;
            int count = in.read(buffer);
            onSource[0] = false;
            if (count < 0) {
              break;
            }
            out.write(buffer, 0, count);
            copy.written += count;
          }
          onSource[0] = true;
        }
        onSource[0] = false;
      });
    } catch (BlobCopyFailure e) {
      throw e;
    } catch (IOException e) {
      throw copy.failure(step[0], e, onSource[0] ? sourceFs : null, source);
    } catch (RuntimeException e) {
      throw copy.failure(step[0], new IOException(e.getMessage(), e), onSource[0] ? sourceFs : null, source);
    }
  }

  /** Classifies construction only; a status or stream failure cannot establish that a source reference is malformed. */
  private static BlobCopyFailure.Fault sourceLookupFault(Path source, Configuration conf, Throwable failure) {
    URI uri = source.toUri();
    BlobCopyFailure.Fault constructionFault = null;
    for (Throwable cause : ExceptionUtil.getCauses(failure)) {
      if (isAccessDenial(cause)) {
        return BlobCopyFailure.Fault.PLATFORM;
      }
      if (constructionFault == null) {
        constructionFault = constructionFaultOf(uri, cause, conf);
      }
    }
    // A nested access denial takes priority over a reference failure reported by an outer wrapper.
    if (constructionFault != null) {
      return constructionFault;
    }
    if (StorageReferenceRules.isMalformedConstruction(uri, failure, conf)) {
      return BlobCopyFailure.Fault.CONTENT;
    }
    return BlobCopyFailure.Fault.TRANSIENT;
  }

  /** The fault {@code cause} gives the construction of a reference to {@code uri}, or null when it tells nothing. */
  private static BlobCopyFailure.Fault constructionFaultOf(URI uri, Throwable cause, Configuration conf) {
    if (cause instanceof UnsupportedFileSystemException) {
      return StorageReferenceRules.isServedScheme(uri.getScheme(), conf) ? BlobCopyFailure.Fault.PLATFORM : BlobCopyFailure.Fault.CONTENT;
    }
    if (cause instanceof ClassNotFoundException || cause instanceof NoClassDefFoundError) {
      // the scheme's configured implementation is not on the classpath (a connector jar absent): the platform's fault
      return BlobCopyFailure.Fault.PLATFORM;
    }
    if (StorageReferenceRules.isInvalidAuthorityFailure(uri, cause)) {
      return BlobCopyFailure.Fault.CONTENT;
    }
    return null;
  }

}
