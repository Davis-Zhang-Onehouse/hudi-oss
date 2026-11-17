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

package org.apache.hudi.utilities.unstructured.embedding.image;

import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SourceFailures;

import javax.imageio.stream.ImageInputStreamImpl;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

/**
 * An {@link javax.imageio.stream.ImageInputStream} over a {@link SeekableBlob}, with no cache: a seek moves the underlying
 * stream, so a reader seeking back in a large OUT_OF_LINE file re-reads storage instead of holding the file on the heap (the
 * failure of {@code MemoryCacheImageInputStream}). A storage failure surfaces as a
 * {@link BlobReadException}, which stays in the cause chain of the reader's own exception; a {@link BlobTruncatedException} of
 * the blob read (the file is shorter than its recorded range, as the blob source's own range checks found) is not one: it passes
 * through as a property of the stored data, in data prep and in compute alike. A bare {@link EOFException} of the storage
 * client is always a storage failure. Closing it closes the blob.
 */
public class SeekableBlobImageInputStream extends ImageInputStreamImpl {

  private final SeekableBlob blob;
  /** Reused by {@link #read()}: readers call it per byte while parsing headers. */
  private final byte[] single = new byte[1];
  /** The most bytes this stream serves in total; {@link Long#MAX_VALUE} for no bound. */
  private final long maxBytesRead;
  private long bytesRead;

  /**
   * A stream with no bound on the bytes it serves, passing a short blob's {@link BlobTruncatedException} through as the data's;
   * compute reads a stored input through {@link #forStoredInput}.
   */
  public SeekableBlobImageInputStream(SeekableBlob blob) {
    this(blob, Long.MAX_VALUE);
  }

  /**
   * The stream compute reads a STORED input through: a storage failure of a read or seek through this stream, a bare
   * {@link EOFException} of the storage client included (a premature EOF, a connection that ended early), surfaces as a
   * {@link BlobReadException}, so it is retried. A stored copy shorter than its recorded range ({@link BlobTruncatedException} of the
   * blob source's own range checks) passes through: it is the row's fault, the same on every read. An EOF the image
   * reader raises itself at the end of intact bytes (a body cut short) stays a property of the data.
   */
  public static SeekableBlobImageInputStream forStoredInput(SeekableBlob blob) {
    return new SeekableBlobImageInputStream(blob, Long.MAX_VALUE);
  }

  /** Opens the seekable blob of a stored input; the storage access {@link #openStoredInput} wraps. */
  @FunctionalInterface
  public interface StoredInputOpener {
    SeekableBlob open() throws IOException;
  }

  /**
   * Opens a STORED input through {@code stored} and reads it through {@link #forStoredInput}. No image byte has been decoded when
   * the open runs, so a failure it raises is the storage's or the stored input's, never the image's: a checked {@link IOException}
   * (an {@link EOFException} of a dropped connection) and a {@link RuntimeException} carrying an {@link IOException} in its cause
   * chain (a storage client reporting unchecked, e.g. HoodieIOException) surface as a {@link BlobReadException}, so the input is
   * retried. Three failures pass through unchanged, because they are not the storage's: a content failure of the stored input
   * ({@link SourceFailures#isContentFailure}: a malformed reference, {@link BlobReferenceException}; a stored file confirmed
   * missing; a stored object shorter than its recorded offset, {@link BlobTruncatedException}), which the caller classifies
   *, and a {@link RuntimeException} with no {@link IOException} under it (a programming error).
   */
  public static SeekableBlobImageInputStream openStoredInput(StoredInputOpener stored) throws IOException {
    SeekableBlob blob;
    try {
      blob = stored.open();
    } catch (IOException e) {
      throw SourceFailures.isContentFailure(e) ? e : new BlobReadException(e);
    } catch (RuntimeException e) {
      if (SourceFailures.isContentFailure(e)) {
        throw e;
      }
      if (carriesIoException(e)) {
        throw new BlobReadException(e);
      }
      throw e;
    }
    return forStoredInput(blob);
  }

  private static boolean carriesIoException(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
      if (cause instanceof IOException) {
        return true;
      }
    }
    return false;
  }

  /**
   * A stream that fails with {@link ImageHeaderException} once more than {@code maxBytesRead} bytes have been read in
   * total, wherever they lie in the blob: data prep reads only a header and bounds what a hostile header can make the
   * reader consume.
   */
  public SeekableBlobImageInputStream(SeekableBlob blob, long maxBytesRead) {
    this.blob = blob;
    this.maxBytesRead = maxBytesRead;
  }

  @Override
  public int read() throws IOException {
    int n = read(single, 0, 1);
    return n < 0 ? -1 : single[0] & 0xff;
  }

  @Override
  public int read(byte[] buffer, int offset, int len) throws IOException {
    checkClosed();
    Objects.checkFromIndexSize(offset, len, buffer.length);
    bitOffset = 0;
    if (len == 0) {
      return 0;
    }
    if (bytesRead >= maxBytesRead) {
      throw new ImageHeaderException("image header not found within " + maxBytesRead + " bytes read");
    }
    len = (int) Math.min(len, maxBytesRead - bytesRead);
    // ImageInputStreamImpl's primitive reads expect their bytes in one call. Storage may return
    // a partial read (for example at an S3 response boundary), so fill the existing caller buffer.
    int total = 0;
    while (total < len) {
      int n;
      try {
        n = blob.read(buffer, offset + total, len - total);
      } catch (IOException e) {
        throw readFailure(e);
      }
      if (n < 0) {
        break;
      }
      streamPos += n;
      bytesRead += n;
      total += n;
    }
    return total == 0 ? -1 : total;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkClosed();
    if (pos < flushedPos) {
      throw new IndexOutOfBoundsException("pos < flushedPos");
    }
    bitOffset = 0;
    try {
      blob.seek(Math.min(pos, blob.length()));
    } catch (IOException e) {
      throw readFailure(e);
    }
    streamPos = pos;
  }

  /**
   * A failure of the blob read: a {@link BlobReadException}, except a {@link BlobTruncatedException} (the stored object is shorter
   * than its recorded range: a property of the data, the same on every attempt), which passes through.
   */
  private IOException readFailure(IOException e) {
    return e instanceof BlobTruncatedException ? e : new BlobReadException(e);
  }

  @Override
  public long length() {
    return blob.length();
  }

  @Override
  public void close() throws IOException {
    super.close();
    blob.close();
  }
}
