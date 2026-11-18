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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.FilePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingException;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceKind;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.apache.pdfbox.util.Version;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * The production pdf {@link FilePreprocessor}: adapts the {@link PdfChunker} contract to the embedding framework.
 * <ul>
 *   <li>an OK {@link PdfChunk} becomes a text {@link Chunk} with its token count and the aux {@code page_start},
 *       {@code page_end}, {@code overlap_chars}; the LAST chunk of a file that lost content also carries
 *       {@code prep_loss} = the {@code PAGES_LOST …} summary;</li>
 *   <li>a FAILED chunker marker becomes a {@link PreprocessingException} carrying its status detail (the framework
 *       quarantines the file, stage prep); a NO_TEXT marker ends the iterator without a chunk (the framework quarantines the
 *       file as empty);</li>
 *   <li>the framework numbers the chunks in emission order, which is the chunker's {@code chunk_id} order.</li>
 * </ul>
 * {@link #name()} is the versioned implementation identity of the preprocessing, which identifies it in logs and error
 * text: it changes whenever {@link #IMPLEMENTATION_VERSION} is bumped, which every change to emitted records must do. The
 * name also records the PDFBox version. A PDFBox upgrade that changes the emitted records must bump
 * {@link #IMPLEMENTATION_VERSION} like any other change; the only automatic check is the pinned-records test, over one
 * synthetic PDF.
 */
public class PdfFilePreprocessor implements FilePreprocessor {

  /** Bump on ANY change to the records this preprocessor emits for the same bytes and config. */
  public static final int IMPLEMENTATION_VERSION = 2;
  public static final String NAME_PREFIX = "pdf-chunker/";
  public static final String AUX_PAGE_START = "page_start";
  public static final String AUX_PAGE_END = "page_end";
  public static final String AUX_OVERLAP_CHARS = "overlap_chars";
  public static final String AUX_PREP_LOSS = "prep_loss";
  /** The clause of {@link #name()} that records the PDFBox version. */
  private static final String PDFBOX_CLAUSE = ";pdfbox=";

  /** The runtime PDFBox version, read once from a jar resource. */
  private static final String PDFBOX_VERSION = Version.getVersion();

  private final PdfChunker chunker;
  private final String name;
  private final PdfChunkerConfig.Cache config = new PdfChunkerConfig.Cache();

  public PdfFilePreprocessor() {
    this(new PdfBoxChunker());
  }

  PdfFilePreprocessor(PdfChunker chunker) {
    this(chunker, PDFBOX_VERSION);
  }

  /** {@code pdfboxVersion} is the PDFBox version {@link #name()} records: the runtime one, or another one in tests. */
  PdfFilePreprocessor(PdfChunker chunker, String pdfboxVersion) {
    this.chunker = chunker;
    this.name = NAME_PREFIX + IMPLEMENTATION_VERSION + PDFBOX_CLAUSE + pdfboxVersion;
  }

  /** {@code pdf-chunker/<IMPLEMENTATION_VERSION>;pdfbox=<PDFBox version>}: the runtime PDFBox unless a test set another. */
  @Override
  public String name() {
    return name;
  }

  @Override
  public SourceKind sourceKind() {
    return SourceKind.DOCUMENT;
  }

  @Override
  public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
    PdfChunkerConfig.from(props, tokenizer);
  }

  @Override
  public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) {
    String logPath = file.getSourcePathSha() + "." + file.getFileExtension();
    PdfChunkerConfig cfg;
    try {
      cfg = config.get(props, tokenizer);
    } catch (IllegalArgumentException e) {
      // invalid props are the job's configuration error: the round fails instead of every file being quarantined
      throw new PreprocessorConfigurationException(e.getMessage(), e);
    }
    ClosableIterator<PdfChunk> chunks = chunker.open(bytes::open, new PdfFileMeta(file.getSourcePathSha(), logPath), cfg);
    return new ChunkAdapter(chunks);
  }

  /** Maps contract records onto framework chunks. */
  private static final class ChunkAdapter implements ClosableIterator<Chunk> {
    private final ClosableIterator<PdfChunk> chunks;
    private Chunk next;
    private boolean ended;

    ChunkAdapter(ClosableIterator<PdfChunk> chunks) {
      this.chunks = chunks;
    }

    @Override
    public boolean hasNext() {
      if (next == null && !ended) {
        if (!chunks.hasNext()) {
          ended = true;
        } else {
          PdfChunk chunk = chunks.next();
          switch (chunk.status()) {
            case OK:
              next = toChunk(chunk);
              break;
            case FAILED:
              ended = true;
              throw new PreprocessingException(chunk.statusDetail());
            case NO_TEXT:
              ended = true;                              // the framework quarantines the file as empty
              break;
            default:
              throw new UnsupportedOperationException("Unknown PdfChunk status " + chunk.status());
          }
        }
      }
      return next != null;
    }

    @Override
    public Chunk next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Chunk chunk = next;
      next = null;
      return chunk;
    }

    @Override
    public void close() {
      chunks.close();
    }

    private static Chunk toChunk(PdfChunk chunk) {
      Map<String, String> aux = new HashMap<>();
      aux.put(AUX_PAGE_START, String.valueOf(chunk.pageStart()));
      aux.put(AUX_PAGE_END, String.valueOf(chunk.pageEnd()));
      aux.put(AUX_OVERLAP_CHARS, String.valueOf(chunk.overlapChars()));
      if (chunk.statusDetail() != null) {
        aux.put(AUX_PREP_LOSS, chunk.statusDetail());
      }
      return new Chunk(EmbeddingTableSchema.MODALITY_TEXT, chunk.text(), chunk.tokenCount(), aux);
    }
  }
}
