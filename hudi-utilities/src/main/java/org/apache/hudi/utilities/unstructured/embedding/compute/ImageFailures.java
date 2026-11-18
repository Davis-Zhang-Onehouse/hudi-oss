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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderException;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;

import javax.imageio.IIOException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * The outcome of an image input whose preparation ({@link EmbeddingInput#image()}) failed, per the
 * {@link EmbeddingClient} image contract, the one classification every multimodal client uses:
 * <ul>
 *   <li>a {@link BlobReferenceException} anywhere in the cause chain, whatever wraps it (the stored-input reference prep wrote
 *       names no readable range: prep's invariant is broken) → {@link EmbeddingComputePlatformException}, no result;</li>
 *   <li>a stored input confirmed missing or shorter than its recorded range ({@link SourceFailures#isContentFailure}) → PERMANENT,
 *       its text naming the row (its five key columns) and the file, so rows of two files, or of two versions of one file,
 *       never share one text;</li>
 *   <li>else a {@link BlobReadException} anywhere in the cause chain (a storage failure under the reader, a premature EOF of the
 *       stored input's storage read included) → RETRYABLE;</li>
 *   <li>an {@link EOFException} not under one (the reader ran out of the stored input's intact bytes, a body cut short),
 *       whether the reader wrapped the EOF (PNG), let it out bare (JPEG, BMP) or an unchecked wrapper carries it → PERMANENT;</li>
 *   <li>a refusal, an unreadable header or corrupt pixel data → PERMANENT;</li>
 *   <li>every PERMANENT text names the row (its five key columns), so rows of two files, or of two versions of one file,
 *       never share one text and alike failures of one call are never read as the provider rejecting the request;</li>
 *   <li>any other {@link IOException}, also under an unchecked wrapper (UncheckedIOException, HoodieIOException) → RETRYABLE;
 *       any other RuntimeException (e.g. the raster budget assertion) → PERMANENT.</li>
 * </ul>
 */
public final class ImageFailures {

  private ImageFailures() {
  }

  /**
   * The result of {@code input}, whose image preparation threw {@code failure}. A RETRYABLE result's text is cut to
   * {@link EmbeddingTableSchema#MAX_ERROR_CHARS} on a code-point boundary ({@link EmbeddingTableSchema#truncateError}), since
   * {@link #describeTransient} may quote a storage client's long message.
   *
   * @throws EmbeddingComputePlatformException for a malformed stored-input reference: no result, the run fails
   */
  public static EmbeddingResult result(EmbeddingInput input, Exception failure) {
    BlobReferenceException reference = find(failure, BlobReferenceException.class);
    if (reference != null) {
      throw new EmbeddingComputePlatformException("the stored input reference of " + rowOf(input) + " is malformed: "
          + reference.getMessage() + "; prep wrote it, so the embedding table's invariant is broken: the compute commit fails and "
          + "the next run resumes from the same checkpoint", failure);
    }
    if (SourceFailures.isContentFailure(failure)) {
      IOException content = find(failure, FileNotFoundException.class);
      if (content == null) {
        content = find(failure, BlobTruncatedException.class);
      }
      return EmbeddingResult.permanent("stored input of " + rowOf(input) + " cannot be read: " + content.getClass().getSimpleName()
          + ": " + content.getMessage());
    }
    return result(failure, rowOf(input) + ": ");
  }

  /** The row's five key columns ({@link EmbeddingInput#describeIdentity}): two versions of one file never share one text. */
  private static String rowOf(EmbeddingInput input) {
    return input.describeIdentity();
  }

  /** The result of a failure that is not the stored input's own: a PERMANENT text starts with {@code row}, so no two rows share one. */
  private static EmbeddingResult result(Exception failure, String row) {
    if (isPermanent(failure)) {
      return EmbeddingResult.permanent(EmbeddingTableSchema.truncateError(row + (failure instanceof IOException
          && failure.getMessage() != null ? failure.getMessage()
          : "image preparation failed: " + failure.getClass().getSimpleName() + ": " + failure.getMessage())));
    }
    return EmbeddingResult.retryable(EmbeddingTableSchema.truncateError("cannot read the image: " + describeTransient(failure)));
  }

  private static boolean isPermanent(Exception failure) {
    if (BlobReadException.isStorageFailure(failure)) {
      return false;
    }
    IOException firstIo = null;
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      if (cause instanceof EOFException) {
        return true;
      }
      if (firstIo == null && cause instanceof IOException) {
        firstIo = (IOException) cause;
      }
    }
    if (firstIo == null) {
      return true;
    }
    return firstIo instanceof ImageRefusedException || firstIo instanceof ImageHeaderException || firstIo instanceof IIOException;
  }

  /** The first cause of {@code failure} (itself included) of {@code type}, or null; a cause chain that loops ends. */
  private static <T extends Throwable> T find(Throwable failure, Class<T> type) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return type.cast(cause);
      }
    }
    return null;
  }

  /**
   * The storage failure under the reader when there is one, else the failure itself. A storage failure's message names only its
   * direct cause, which may be a wrapper (the storage layer's "Exception creating input stream"); when the innermost cause below
   * it is another throwable with a message the text does not already hold, that root cause is named too.
   */
  private static String describeTransient(Exception failure) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      if (cause instanceof BlobReadException) {
        String text = cause.getMessage();
        Throwable root = rootCause(cause);
        if (root != cause.getCause() && root.getMessage() != null && !text.contains(root.getMessage())) {
          text = text + " (root cause " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
        }
        return text;
      }
    }
    return failure.getClass().getSimpleName() + ": " + failure.getMessage();
  }

  /** The innermost cause of {@code failure}, or the last one before its cause chain loops. */
  private static Throwable rootCause(Throwable failure) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Throwable root = failure;
    while (visited.add(root) && root.getCause() != null && !visited.contains(root.getCause())) {
      root = root.getCause();
    }
    return root;
  }
}
