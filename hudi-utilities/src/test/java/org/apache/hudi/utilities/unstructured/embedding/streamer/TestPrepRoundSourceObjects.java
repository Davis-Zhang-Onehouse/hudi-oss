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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link PrepRound#referenceSourceObjects}: only an out-of-line value without a path (what the source emits for an object above
 * its inline ceiling) becomes an unmanaged reference to the row's source_file_path with no range; an inline value, an
 * out-of-line value that has a path, and a null blob are kept as they are.
 */
@Tag("functional")
class TestPrepRoundSourceObjects extends SparkClientFunctionalTestHarness {

  private static Row row(String name, Row blob) {
    String path = "bucketfs://landing/" + name;
    return RowFactory.create("sha-" + name, 1234L, path, name, "pdf", 10L, "application/pdf", blob);
  }

  private static Row outOfLine(String path, Long offset, Long length, Boolean managed) {
    return RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, RowFactory.create(path, offset, length, managed));
  }

  @Test
  void onlyAnOutOfLineValueWithoutAPathIsPointedAtItsObject() {
    byte[] inline = "%PDF inline".getBytes(StandardCharsets.UTF_8);
    List<Row> input = Arrays.asList(
        row("pending.pdf", outOfLine(null, 0L, 10L, true)),
        row("inline.pdf", RowFactory.create(HoodieSchema.Blob.INLINE, inline, null)),
        row("copied.pdf", outOfLine(".hoodie/.blob/pdf/x.bin", 0L, 10L, true)),
        row("absolute.pdf", outOfLine("s3a://elsewhere/y.pdf", 2L, 5L, false)),
        row("null.pdf", null));

    Map<String, Row> blobs = new TreeMap<>();
    for (Row out : PrepRound.referenceSourceObjects(spark().createDataFrame(input, BlobTableSchema.schema())).collectAsList()) {
      blobs.put(out.getAs(BlobTableSchema.SOURCE_FILE_NAME), out.getAs(BlobTableSchema.BLOB));
    }

    Row pending = blobs.get("pending.pdf");
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, pending.getAs(HoodieSchema.Blob.TYPE));
    assertNull(pending.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD));
    Row reference = pending.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
    assertEquals("bucketfs://landing/pending.pdf", reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH));
    assertNull(reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET), "no range: the object is read whole");
    assertNull(reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH));
    assertEquals(Boolean.FALSE, reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED));

    Row kept = blobs.get("inline.pdf");
    assertEquals(HoodieSchema.Blob.INLINE, kept.getAs(HoodieSchema.Blob.TYPE));
    assertArrayEquals(inline, (byte[]) kept.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD));
    assertNull(kept.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE));
    assertEquals(outOfLine(".hoodie/.blob/pdf/x.bin", 0L, 10L, true).getStruct(2), blobs.get("copied.pdf").getStruct(2));
    assertEquals(outOfLine("s3a://elsewhere/y.pdf", 2L, 5L, false).getStruct(2), blobs.get("absolute.pdf").getStruct(2));
    assertNull(blobs.get("null.pdf"));
  }
}
