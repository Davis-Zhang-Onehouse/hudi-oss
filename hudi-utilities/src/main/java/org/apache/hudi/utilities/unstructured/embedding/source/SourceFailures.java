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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.utilities.sources.blob.BlobObjectCopier;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Tells a storage failure that is a file's own fault from one a later run may not see: a
 * source that is gone ({@link FileNotFoundException}: at open {@code BlobBytesSource} re-checks it, and a file found present becomes
 * an access denial; a FileNotFoundException of a later read, e.g. an S3A lazy open, is re-checked the same way),
 * short ({@link BlobTruncatedException}, raised only by {@code BlobBytesSource}'s own range checks) or whose blob
 * row names no readable range ({@link BlobReferenceException}) gives the same answer on every read, so it is a content failure of
 * its file (its quarantine entry); any other storage failure is an interruption ({@code SourceReadException} of data prep). A
 * bare {@link java.io.EOFException} is not a content failure: Hadoop also reports a dropped connection as one. Used by
 * {@link BlobBytesSource#isTransient}.
 */
public final class SourceFailures {

  private SourceFailures() {
  }

  /**
   * Whether {@code failure}'s cause chain says the file itself is at fault: its source is gone or short, or its row malformed. An
   * access denial anywhere in the chain ({@link BlobObjectCopier#isAccessDenial}: java.nio.file.AccessDeniedException, which is
   * a 403, a KMS denial or a file there but unreadable; Hadoop's AccessControlException; the GCS connector's 403 response,
   * unless it names a rate-limit reason) is never the file's fault, even when it carries the storage's FileNotFoundException.
   */
  public static boolean isContentFailure(Throwable failure) {
    boolean content = false;
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      if (BlobObjectCopier.isAccessDenial(cause)) {
        return false;
      }
      content |= cause instanceof FileNotFoundException || cause instanceof BlobTruncatedException
          || cause instanceof BlobReferenceException;
    }
    return content;
  }
}
