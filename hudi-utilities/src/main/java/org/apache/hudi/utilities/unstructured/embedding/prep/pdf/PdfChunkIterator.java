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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.ChunkPacker;
import org.apache.hudi.utilities.unstructured.embedding.text.PackedChunk;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenBudget;

import org.apache.spark.TaskContext;

import java.util.NoSuchElementException;

/**
 * The chunk iterator of ONE file. It pulls pages from a {@link PageSource} only as
 * far as the pull rule needs, normalizes them, lets the {@link ChunkPacker} cut chunks, and HOLDS BACK one cut
 * chunk so the last OK chunk can carry the loss summary. Everything runs on the caller's thread inside
 * {@code hasNext()}/{@code next()}; nothing runs between calls and no thread is started.
 *
 * <p>Isolation: a page failure skips that page with a WARN; a file-level failure ends the file — with a FAILED
 * marker when no OK chunk was cut yet, else by flushing the pending text (unless the tokenizer failed) and signalling the
 * loss on the last chunk. {@link VirtualMachineError}s other than {@link StackOverflowError},
 * {@link LinkageError}s and interruption propagate. At most {@value #MAX_PAGE_WARNINGS} page-level WARN lines
 * per file, then one summary WARN.
 *
 * <p>A failure that fails the prep round ({@link PrepFailures#roundFailure}) is never isolated, at the file or at the
 * page: a storage failure of the source a later run can still read, a platform failure (a local scratch disk that cannot be
 * written), a configuration error, or a RETRYABLE tokenizer failure after the tokenizer's own retries. The iterator closes its
 * page source and throws it as it was met, whatever wrapped it, so neither a FAILED marker (a quarantine entry) nor a file
 * truncated behind a loss summary is committed for it. Everything else a PDFBox call throws on an input is the input's own failure.
 */
public final class PdfChunkIterator implements ClosableIterator<PdfChunk> {

  /** Receives the iterator's WARN lines; the production sink is the chunker's logger. */
  @FunctionalInterface
  interface WarnSink {
    void warn(String line);
  }

  static final int MAX_PAGE_WARNINGS = 10;

  private final InputStreamSupplier source;
  private final PdfFileMeta meta;
  private final PdfChunkerConfig config;
  private final PageSourceFactory factory;
  private final WarnSink warnings;
  private final TextNormalizer normalizer;
  private final ChunkPacker packer;
  private final String tokenizerId;

  private PageSource pageSource;
  private boolean started;
  private boolean sourceExhausted;
  private boolean flushing;
  private boolean ended;
  private boolean closed;
  private PdfChunk held;
  private PdfChunk ready;
  private long ordinal;
  private PdfFailureCode fileError;
  private String fileErrorDetail;
  private int pageWarnings;
  private long pagesSeen;
  private long pagesFailed;

  PdfChunkIterator(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config, PageSourceFactory factory,
                   WarnSink warnings) {
    this.source = source;
    this.meta = meta;
    this.config = config;
    this.factory = factory;
    this.warnings = warnings;
    this.normalizer = new TextNormalizer(Integer.MAX_VALUE);
    this.packer = new ChunkPacker(new TokenBudget(config.tokenizer(), config.maxTokens(), config.maxWords(),
        config.windowChars(), PdfFileFailure.PACK_FAILURES), config.overlapTokens(), config.locale());
    this.tokenizerId = config.tokenizer().id();
  }

  @Override
  public boolean hasNext() {
    while (ready == null && !ended && !closed) {
      step();
    }
    return ready != null;
  }

  @Override
  public PdfChunk next() {
    if (!hasNext()) {
      throw new NoSuchElementException("no more chunks for source_path_sha " + meta.sourcePathSha());
    }
    PdfChunk chunk = ready;
    ready = null;
    return chunk;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      ready = null;
      held = null;
      closeSource();
    }
  }

  // ------------------------------------------------------------------ the pull loop

  private void step() {
    checkInterrupted();
    try {
      if (!started) {
        start();
      } else if (!sourceExhausted && packer.needsMoreText()) {
        pullPage();
      } else {
        PackedChunk cut = packer.cut(sourceExhausted);
        if (cut == null) {
          finish();
        } else {
          accept(cut);
        }
      }
    } catch (PdfChunkerInterruptedException e) {
      throw e;
    } catch (PdfFileFailure e) {
      checkInterrupted(e);
      rethrowRoundFailure(e);
      failFile(e.code(), e);
    } catch (Exception | StackOverflowError e) {
      checkInterrupted(e);
      rethrowRoundFailure(e);
      failFile(PdfFailureCode.INTERNAL, e);
    }
  }

  private void start() throws Exception {
    started = true;
    pageSource = factory.create(source, meta, config);
    pageSource.open();
  }

  private void pullPage() {
    if (!pageSource.hasMorePages()) {
      sourceExhausted = true;
      return;
    }
    int page = (int) ++pagesSeen;
    PageText raw;
    try {
      raw = pageSource.next();
    } catch (PdfFileFailure | PdfChunkerInterruptedException e) {
      throw e;
    } catch (Exception | StackOverflowError e) {
      checkInterrupted(e);
      rethrowRoundFailure(e);
      pageFailed(page, e);
      return;
    }
    TextNormalizer.Result normalized = normalizer.normalize(raw.text());
    if (normalized.unpairedSurrogates() > 0) {
      pageWarning(null, page, normalized.unpairedSurrogates() + " unpaired surrogates replaced by U+FFFD", null);
    }
    if (!normalized.text().isEmpty()) {
      packer.appendPage(normalized.text(), page);
    }
  }

  private void pageFailed(int page, Throwable cause) {
    pagesFailed++;
    pageWarning(PdfFailureCode.PAGE_ERROR, page, null, cause);
  }

  private void accept(PackedChunk cut) {
    PdfChunk chunk = new PdfChunk(meta.sourcePathSha(), ordinal++, cut.text(), cut.tokenCount(), tokenizerId, cut.wordCount(),
        cut.overlapChars(), cut.firstMark(), cut.lastMark(), PdfChunk.Status.OK, null);
    if (held != null) {
      ready = held;                                    // a chunk is returned once the next one is cut
    }
    held = chunk;
  }

  /** A file-level failure. */
  private void failFile(PdfFailureCode code, Throwable cause) {
    if (fileError == null) {
      fileError = code;
      fileErrorDetail = code.format(cause);
    }
    if (held == null) {
      end(PdfChunk.marker(meta.sourcePathSha(), tokenizerId, PdfChunk.Status.FAILED, fileErrorDetail));
    } else if (code == PdfFailureCode.TOKENIZER_ERROR || flushing) {
      end(held.withStatusDetail(lossSummary()));
    } else {
      flushing = true;                                 // cut the text of the pages already extracted, then end
      sourceExhausted = true;
    }
  }

  /** The end of the input: the last chunk with its loss summary, or the file's single marker row. */
  private void finish() {
    if (held != null) {
      end(lost() ? held.withStatusDetail(lossSummary()) : held);
      return;
    }
    if (pagesFailed > 0) {
      end(PdfChunk.marker(meta.sourcePathSha(), tokenizerId, PdfChunk.Status.FAILED, PdfFailureCode.ALL_PAGES_FAILED.name()
          + ": no page yielded text, " + lossSummary()));
    } else {
      end(PdfChunk.marker(meta.sourcePathSha(), tokenizerId, PdfChunk.Status.NO_TEXT, null));
    }
  }

  private void end(PdfChunk last) {
    ready = last;
    held = null;
    ended = true;
    // the summary also follows suppressed page WARNs on an OK, loss-free file
    if (lost() || last.status() != PdfChunk.Status.OK || pageWarnings > MAX_PAGE_WARNINGS) {
      warnings.warn("PDF-FILE source_path_sha=" + meta.sourcePathSha() + " path=" + meta.sourcePath() + " status=" + last.status() + " "
          + (lost() ? lossSummary() : "pages=" + pagesSeen + (last.status() == PdfChunk.Status.OK ? "" : " (no text)"))
          + " pageWarnings=" + pageWarnings
          + (fileErrorDetail == null ? "" : " error=" + fileErrorDetail));
    }
    closeSource();
  }

  private boolean lost() {
    return pagesFailed > 0 || fileError != null;
  }

  /** The durable loss signal the last OK chunk carries. */
  private String lossSummary() {
    return "PAGES_LOST failed=" + pagesFailed + " file_error=" + (fileError == null ? "none" : fileError.name());
  }

  private void pageWarning(PdfFailureCode code, long page, String detail, Throwable cause) {
    pageWarnings++;
    if (pageWarnings <= MAX_PAGE_WARNINGS) {
      warnings.warn("PDF-PAGE source_path_sha=" + meta.sourcePathSha() + " path=" + meta.sourcePath() + " page=" + page
          + (code == null ? "" : " code=" + code) + (detail == null ? "" : " " + detail)
          + (cause == null ? "" : " " + cause.getClass().getName() + ": " + cause.getMessage()));
    }
  }

  // ------------------------------------------------------------------ interruption and cleanup

  private void checkInterrupted() {
    checkInterrupted(null);
  }

  /**
   * Throws when the thread or the Spark task is interrupted, or when {@code failure} was caused by an interrupt (an
   * {@link java.io.InterruptedIOException} or {@link java.nio.channels.ClosedByInterruptException} whose library cleared
   * the flag).
   */
  private void checkInterrupted(Throwable failure) {
    TaskContext task = TaskContext.get();
    boolean interrupted = Thread.currentThread().isInterrupted() || (task != null && task.isInterrupted());
    for (Throwable t = failure; t != null && !interrupted; t = t.getCause() == t ? null : t.getCause()) {
      interrupted = t instanceof java.io.InterruptedIOException || t instanceof java.nio.channels.ClosedByInterruptException
          || t instanceof InterruptedException;
    }
    if (interrupted) {
      closeSource();
      throw new PdfChunkerInterruptedException("interrupted while chunking source_path_sha " + meta.sourcePathSha());
    }
  }

  /**
   * Ends the file and throws the failure of {@code failure}'s cause or suppressed chain that fails the round, as it was met;
   * returns for a failure of the file's own input.
   */
  private void rethrowRoundFailure(Throwable failure) {
    RuntimeException round = PrepFailures.roundFailure(failure);
    if (round != null) {
      ended = true;
      ready = null;
      held = null;
      closeSource();
      throw round;
    }
  }

  /** Closes the page source; a failing close never changes the records already cut. */
  private void closeSource() {
    if (pageSource != null) {
      PageSource toClose = pageSource;
      pageSource = null;
      try {
        toClose.close();
      } catch (RuntimeException e) {
        warnings.warn("PDF-FILE source_path_sha=" + meta.sourcePathSha() + " path=" + meta.sourcePath() + " closing the page source failed: "
            + e.getClass().getName() + ": " + e.getMessage());
      }
    }
  }
}
