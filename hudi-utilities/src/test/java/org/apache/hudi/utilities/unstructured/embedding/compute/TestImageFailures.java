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

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;

import org.junit.jupiter.api.Test;

import javax.imageio.IIOException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The classification of an image input whose stored input could not be read ({@link ImageFailures#result}): a malformed
 * reference fails the run; a missing or short stored file is PERMANENT naming the row; a storage failure's recorded text names
 * the root cause below a wrapper and is cut to {@link EmbeddingTableSchema#MAX_ERROR_CHARS} on a code-point boundary.
 */
public class TestImageFailures {

  private static final String PREFIX = "cannot read the image: ";
  private static final EmbeddingInput INPUT = new EmbeddingInput("f", 0, EmbeddingTableSchema.MODALITY_IMAGE, null);
  /** The five key columns of {@link #INPUT}, as every text naming the row gives them. */
  private static final String ROW = "source_path_sha=f source_modification_time=0 source_file_size=0 modality=image chunk_id=0";

  /**
   * A malformed stored-input reference breaks an invariant prep owns: whatever wraps it, the classification throws the
   * platform failure, naming the row and the reference's message, and gives no result.
   */
  @Test
  public void referenceFaultFailsTheRunAsAPlatformFailure() {
    for (Exception failure : new Exception[] {
        new BlobReferenceException("OUT_OF_LINE blob has null reference"),
        new UncheckedIOException(new BlobReferenceException("OUT_OF_LINE blob has null reference")),
        new BlobReadException(new BlobReferenceException("OUT_OF_LINE blob has null reference"))}) {
      EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
          () -> ImageFailures.result(INPUT, failure), failure.toString());
      assertTrue(e.getMessage().contains(ROW) && e.getMessage().contains("OUT_OF_LINE blob has null reference"),
          e.getMessage());
    }
  }

  /**
   * A well-formed reference whose stored file is confirmed missing, or shorter than its recorded range, is the row's
   * fault: PERMANENT, its text naming the row, so two files never share one text.
   */
  @Test
  public void missingOrShortStoredFileIsPermanentNamingTheRow() {
    EmbeddingResult missing = ImageFailures.result(INPUT, new FileNotFoundException("File /t/a.bin does not exist"));
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, missing.getStatus());
    assertEquals("stored input of " + ROW + " cannot be read: FileNotFoundException: File /t/a.bin does not exist",
        missing.getError());
    EmbeddingResult shortCopy = ImageFailures.result(INPUT, new IIOException("Error reading PNG image data",
        new BlobTruncatedException("Blob file /t/a.bin ended early: expected 10 more bytes")));
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, shortCopy.getStatus());
    assertEquals("stored input of " + ROW + " cannot be read: BlobTruncatedException: Blob file /t/a.bin ended early: "
        + "expected 10 more bytes", shortCopy.getError());
    EmbeddingResult wrapped = ImageFailures.result(INPUT, new HoodieIOException("Exception creating input stream from file: /t/a.bin",
        new FileNotFoundException("File /t/a.bin does not exist")));
    assertEquals(missing.getError(), wrapped.getError(), "the missing file is named whatever wraps it");
    EmbeddingInput other = new EmbeddingInput("g", 0, EmbeddingTableSchema.MODALITY_IMAGE, null);
    assertNotEquals(missing.getError(), ImageFailures.result(other, new FileNotFoundException("File /t/a.bin does not exist")).getError(),
        "two rows never share one text");
    EmbeddingInput otherVersion = new EmbeddingInput("f", 5L, 10L, EmbeddingTableSchema.MODALITY_IMAGE, 0, null);
    assertNotEquals(missing.getError(), ImageFailures.result(otherVersion, new FileNotFoundException("File /t/a.bin does not exist"))
        .getError(), "two versions of one file never share one text");
  }

  /** An access denial carrying a FileNotFoundException (the file is there and unreadable) stays a storage failure. */
  @Test
  public void accessDeniedFileIsNotAMissingFile() {
    AccessDeniedException denied = new AccessDeniedException("/t/a.bin", null, "exists but cannot be opened");
    denied.initCause(new FileNotFoundException("/t/a.bin (Permission denied)"));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, ImageFailures.result(INPUT, new BlobReadException(denied)).getStatus());
  }

  @Test
  public void wrappedOpenFailureNamesItsRootCause() {
    EmbeddingResult result = ImageFailures.result(INPUT, new BlobReadException(
        new HoodieIOException("Exception creating input stream from file: x", new EOFException("Premature EOF"))));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertEquals(PREFIX + "HoodieIOException: Exception creating input stream from file: x (root cause EOFException: Premature EOF)",
        result.getError());
  }

  @Test
  public void directCauseTextIsUnchanged() {
    EmbeddingResult result = ImageFailures.result(INPUT, new BlobReadException(new IOException("Connection reset (simulated)")));
    assertEquals(PREFIX + "IOException: Connection reset (simulated)", result.getError());
  }

  @Test
  public void rootMessageAlreadyInTheWrapperIsNotRepeated() {
    EmbeddingResult result = ImageFailures.result(INPUT, new BlobReadException(
        new HoodieIOException("open failed: timeout (simulated)", new IOException("timeout (simulated)"))));
    assertEquals(PREFIX + "HoodieIOException: open failed: timeout (simulated)", result.getError());
  }

  @Test
  public void rootWithoutAMessageAddsNothing() {
    EmbeddingResult result = ImageFailures.result(INPUT, new BlobReadException(new HoodieIOException("open failed", new EOFException())));
    assertEquals(PREFIX + "HoodieIOException: open failed", result.getError());
  }

  /** A surrogate pair straddling the bound is dropped whole: the text ends one char short, never on a lone high surrogate. */
  @Test
  public void composedTextIsCutBeforeAPairStraddlingTheBound() {
    String error = ImageFailures.result(INPUT, longRootFailure(EmbeddingTableSchema.MAX_ERROR_CHARS - 1)).getError();
    assertEquals(EmbeddingTableSchema.MAX_ERROR_CHARS - 1, error.length());
    assertFalse(Character.isHighSurrogate(error.charAt(error.length() - 1)), "no lone high surrogate at the end");
  }

  /** A surrogate pair ending exactly at the bound is kept whole. */
  @Test
  public void composedTextKeepsAPairEndingAtTheBound() {
    String error = ImageFailures.result(INPUT, longRootFailure(EmbeddingTableSchema.MAX_ERROR_CHARS - 2)).getError();
    assertEquals(EmbeddingTableSchema.MAX_ERROR_CHARS, error.length());
    assertTrue(error.endsWith("😀"), "the pair ending at the bound is kept");
  }

  /** A cause chain that loops on itself below the storage failure still ends, naming the failure once. */
  @Test
  public void selfCausedChainEnds() {
    EmbeddingResult result = ImageFailures.result(INPUT, new BlobReadException(new SelfCausedIOException()));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertEquals(PREFIX + "SelfCausedIOException: self-caused (simulated)", result.getError());
  }

  /** A cause chain that loops through two links (a → b → a) still ends, bare or under a storage failure. */
  @Test
  public void twoLinkCauseLoopEnds() {
    IOException b = new IOException("b");
    IOException a = new IOException("a", b);
    b.initCause(a);
    EmbeddingResult bare = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> ImageFailures.result(INPUT, a));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, bare.getStatus());
    assertEquals(PREFIX + "IOException: a", bare.getError());
    EmbeddingResult underStorage = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> ImageFailures.result(INPUT, new BlobReadException(a)));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, underStorage.getStatus());
    assertEquals(PREFIX + "IOException: a (root cause IOException: b)", underStorage.getError());
  }

  /**
   * A wrapped storage failure whose root message puts the high surrogate of one astral code point at index {@code pairAt} of the
   * recorded text, followed by more than enough text to pass the bound.
   */
  private static BlobReadException longRootFailure(int pairAt) {
    String head = PREFIX + "HoodieIOException: open failed (root cause IOException: ";
    StringBuilder root = new StringBuilder();
    for (int i = head.length(); i < pairAt; i++) {
      root.append('a');
    }
    root.append("😀");
    for (int i = 0; i < 1000; i++) {
      root.append('b');
    }
    return new BlobReadException(new HoodieIOException("open failed", new IOException(root.toString())));
  }

  private static final class SelfCausedIOException extends IOException {
    SelfCausedIOException() {
      super("self-caused (simulated)");
    }

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }
}
