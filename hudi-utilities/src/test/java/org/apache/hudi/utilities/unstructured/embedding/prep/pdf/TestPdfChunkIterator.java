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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parser-free iterator over fake page sources: lifecycle, records and error propagation.
 */
public class TestPdfChunkIterator {

  private static final Tokenizer WORDS = new WordCountTokenizer();
  private static final PdfFileMeta META = new PdfFileMeta("fid-1", "file:///docs/x.pdf");

  /** One page of a fake source: returns text or throws. */
  @FunctionalInterface
  interface FakePage {
    PageText extract() throws Exception;
  }

  static FakePage text(String text) {
    return () -> new PageText(text);
  }

  static FakePage throwing(Throwable failure) {
    return () -> {
      if (failure instanceof Exception) {
        throw (Exception) failure;
      }
      throw (Error) failure;
    };
  }

  /** A page source over a fixed or generated list of pages. */
  static class FakeSource implements PageSource {
    private final IntFunction<FakePage> pages;
    private final int pageCount;
    private final InputStreamSupplier supplier;
    private int next;
    boolean opened;
    boolean closed;

    FakeSource(InputStreamSupplier supplier, int pageCount, IntFunction<FakePage> pages) {
      this.supplier = supplier;
      this.pageCount = pageCount;
      this.pages = pages;
    }

    @Override
    public void open() throws java.io.IOException {
      supplier.open().close();
      opened = true;
    }

    @Override
    public boolean hasMorePages() {
      return next < pageCount;
    }

    @Override
    public PageText next() throws java.io.IOException {
      FakePage page = pages.apply(next++);
      try {
        return page.extract();
      } catch (java.io.IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long stagedBytes() {
      return 0;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private final List<String> warnings = new ArrayList<>();
  private final List<FakeSource> sources = new ArrayList<>();

  private PdfChunkIterator open(PdfChunkerConfig config, List<FakePage> pages) {
    return open(config, pages.size(), pages::get, () -> new ByteArrayInputStream(new byte[1]));
  }

  private PdfChunkIterator open(PdfChunkerConfig config, int pageCount, IntFunction<FakePage> pages,
                                InputStreamSupplier supplier) {
    PdfBoxChunker chunker = new PdfBoxChunker((s, m, c) -> {
      FakeSource source = new FakeSource(s, pageCount, pages);
      sources.add(source);
      return source;
    });
    return chunker.open(supplier, META, config, warnings::add);
  }

  static PdfChunkerConfig config(Tokenizer tokenizer, String... keyValues) {
    TypedProperties props = new TypedProperties();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return PdfChunkerConfig.from(props, tokenizer);
  }

  private static PdfChunkerConfig small() {
    return config(WORDS, EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "20", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
  }

  static String sentences(String prefix, int count) {
    StringBuilder sb = new StringBuilder();
    for (int s = 0; s < count; s++) {
      sb.append("Sentence ").append(prefix).append(s).append(" has six words here. ");
    }
    return sb.toString().trim();
  }

  private static List<PdfChunk> drain(ClosableIterator<PdfChunk> iterator) {
    List<PdfChunk> chunks = new ArrayList<>();
    while (iterator.hasNext()) {
      chunks.add(iterator.next());
    }
    iterator.close();
    return chunks;
  }

  // ------------------------------------------------------------------ lifecycle

  @Test
  public void openDoesNoIo() {
    AtomicBoolean called = new AtomicBoolean();
    PdfChunkIterator iterator = open(small(), 1, i -> text(sentences("a", 2)), () -> {
      called.set(true);
      return new ByteArrayInputStream(new byte[1]);
    });
    assertFalse(called.get(), "open() must not call the supplier");
    assertTrue(sources.isEmpty() || !sources.get(0).opened);
    assertTrue(iterator.hasNext());
    assertTrue(called.get());
    iterator.close();
  }

  @Test
  public void openReturnsHudiClosableIterator() {
    PdfChunker chunker = new PdfBoxChunker((s, m, c) -> new FakeSource(s, 1, i -> text("One sentence.")));
    ClosableIterator<PdfChunk> iterator = chunker.open(() -> new ByteArrayInputStream(new byte[1]), META, small());
    assertEquals("One sentence.", drain(iterator).get(0).text());
  }

  @Test
  public void hasNextIdempotent() {
    PdfChunkIterator iterator = open(small(), Collections.singletonList(text(sentences("a", 6))));
    for (int i = 0; i < 5; i++) {
      assertTrue(iterator.hasNext());
    }
    PdfChunk first = iterator.next();
    assertEquals(0, first.chunkId());
    assertTrue(iterator.hasNext() && iterator.hasNext());
    assertEquals(1, iterator.next().chunkId());
    iterator.close();
  }

  @Test
  public void nextAfterEndThrows() {
    PdfChunkIterator iterator = open(small(), Collections.singletonList(text("Only one.")));
    drain(iterator);
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void noThreadsStarted() {
    int before = Thread.activeCount();
    drain(open(small(), Arrays.asList(text(sentences("a", 20)), text(sentences("b", 20)))));
    assertEquals(before, Thread.activeCount());
  }

  @Test
  public void everyRecordCarriesTheSourcePathSha() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(text(sentences("a", 9)), text(sentences("b", 9)))));
    assertTrue(chunks.size() > 3);
    assertTrue(chunks.stream().allMatch(c -> "fid-1".equals(c.sourcePathSha())));
  }

  @Test
  public void chunkIdsAreConsecutiveFromZero() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(text(sentences("a", 9)), text(sentences("b", 9)))));
    for (int i = 0; i < chunks.size(); i++) {
      assertEquals(i, chunks.get(i).chunkId());
      assertEquals(PdfChunk.Status.OK, chunks.get(i).status());
    }
  }

  @Test
  public void closeTwiceIsNoop() {
    PdfChunkIterator iterator = open(small(), Collections.singletonList(text(sentences("a", 9))));
    assertTrue(iterator.hasNext());
    iterator.close();
    iterator.close();
    assertTrue(sources.get(0).closed);
    assertFalse(iterator.hasNext(), "closing mid-file emits nothing further");
  }

  // ------------------------------------------------------------------ errors

  @Test
  public void outOfMemoryErrorPropagates() {
    PdfChunkIterator iterator = open(small(), Arrays.asList(text(sentences("a", 4)), throwing(new OutOfMemoryError("fake"))));
    assertThrows(OutOfMemoryError.class, () -> drain(iterator));
    iterator.close();
    assertTrue(sources.get(0).closed);
  }

  @Test
  public void failingSourceCloseKeepsTheRecords() {
    // a throwing PageSource.close() must not replace the last OK chunk with a FAILED marker
    PdfBoxChunker chunker = new PdfBoxChunker((s, m, c) -> new FakeSource(s, 1, i -> text(sentences("a", 6))) {
      @Override
      public void close() {
        throw new IllegalStateException("close failed");
      }
    });
    List<PdfChunk> chunks = drain(chunker.open(() -> new ByteArrayInputStream(new byte[1]), META, small(), warnings::add));
    assertTrue(chunks.size() > 1);
    assertTrue(chunks.stream().allMatch(c -> c.status() == PdfChunk.Status.OK));
    assertTrue(warnings.stream().anyMatch(w -> w.contains("closing the page source failed")));
  }

}
