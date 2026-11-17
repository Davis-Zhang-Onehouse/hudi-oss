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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.testutils.HandleCountingFileSystem;
import org.apache.hudi.utilities.unstructured.embedding.input.PreparedInputs;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compute reading a stored input back: the image of a row's {@code prepared_image}, a managed reference relative to the
 * embedding table, is opened as {@code MicroBatchEmbeddingIterator} opens it ({@link SeekableBlobImageInputStream#openStoredInput}
 * over {@link BlobBytesSource#openSeekable}) and prepared by {@link ImagePreparer}; each of its two opens (plan, decode) is
 * closed, on the main path and when the image is refused, unreadable or its read fails.
 */
public class TestImagePreparerStoredInputHandleRelease {

  private static final String STORED = ".hoodie/.blob/p1/stored.bin";

  @TempDir
  Path tempDir;

  private Path table;

  @BeforeEach
  public void setUp() throws IOException {
    HandleCountingFileSystem.reset();
    table = tempDir.resolve("embedding");
    Files.createDirectories(table.resolve(STORED).getParent());
  }

  @AfterEach
  public void resetRegistry() {
    HandleCountingFileSystem.reset();
  }

  @Test
  public void mainPathClosesThePlanAndDecodeStreams() throws IOException {
    PreparedImage prepared = prepare(ImageFixtures.image("png", 50, 30), spec(ImagePreprocessingSpec.DEFAULT_MAX_PIXELS));
    assertTrue(prepared.getEncoded().length > 0);
    assertEquals(2, HandleCountingFileSystem.opened(), "one stream for the plan and one for the decode");
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "stored-input streams left open by the main path");
  }

  @Test
  public void refusedImageClosesThePlanStream() {
    assertThrows(ImageRefusedException.class, () -> prepare(ImageFixtures.image("png", 50, 30), spec(100)));
    assertEquals(1, HandleCountingFileSystem.opened());
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "stored-input streams left open by a refused image");
  }

  @Test
  public void unreadableImageClosesItsStreams() {
    assertThrows(IOException.class, () -> prepare(ImageFixtures.headerCorrupt("png"), spec(ImagePreprocessingSpec.DEFAULT_MAX_PIXELS)));
    assertThrows(IOException.class, () -> prepare(ImageFixtures.garbage(), spec(ImagePreprocessingSpec.DEFAULT_MAX_PIXELS)));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "stored-input streams left open by an unreadable image");
  }

  @Test
  public void failedStoredReadClosesItsStream() throws IOException {
    byte[] png = ImageFixtures.image("png", 50, 30);
    Files.write(table.resolve(STORED), png);
    HandleCountingFileSystem.inject(table.resolve(STORED), HandleCountingFileSystem.Fault.READ, 1000);
    assertThrows(IOException.class, () -> prepareWritten(png.length, spec(ImagePreprocessingSpec.DEFAULT_MAX_PIXELS)));
    assertEquals(Collections.emptyList(), HandleCountingFileSystem.unclosed(), "stored-input streams left open by a failed read");
  }

  private PreparedImage prepare(byte[] image, ImagePreprocessingSpec spec) throws IOException {
    Files.write(table.resolve(STORED), image);
    return prepareWritten(image.length, spec);
  }

  private PreparedImage prepareWritten(long length, ImagePreprocessingSpec spec) throws IOException {
    Configuration conf = HandleCountingFileSystem.install(new Configuration());
    BlobBytesSource source = new BlobBytesSource(HandleCountingFileSystem.url(table), HadoopFSUtils.getStorageConf(conf));
    Row reference = new GenericRowWithSchema(new Object[] {STORED, 0L, length, true}, PreparedInputs.REFERENCE_STRUCT);
    Row stored = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, PreparedInputs.BLOB_STRUCT);
    return new ImagePreparer(spec).prepare(() -> SeekableBlobImageInputStream.openStoredInput(() -> source.openSeekable(stored)));
  }

  private static ImagePreprocessingSpec spec(long maxPixels) {
    return new ImagePreprocessingSpec(ImagePreprocessingSpec.DEFAULT_LONG_SIDE, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES,
        maxPixels, ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES, InProcessImageDecoder.class.getName(),
        ImagePreprocessingSpec.ENCODING_PNG, ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY);
  }
}
