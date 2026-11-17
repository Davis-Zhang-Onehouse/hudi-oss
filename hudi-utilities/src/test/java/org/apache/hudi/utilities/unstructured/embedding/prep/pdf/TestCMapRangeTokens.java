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

import org.apache.fontbox.cmap.CMap;
import org.apache.fontbox.cmap.CMapParser;
import org.apache.pdfbox.io.RandomAccessReadBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Malformed CMaps are tokenized as CMapParser tokenizes them, so their bfrange codes are counted. */
public class TestCMapRangeTokens {

  /** One code more than the cap: 0x000000 to 0x040000. */
  private static final String OVER_CAP_RANGE = "<000000> <040000> <0041> endbfrange";
  private static final int LAST_CODE = 0x040000;

  @ParameterizedTest
  @ValueSource(strings = {
      "(\\) 1 beginbfrange ",                    // a backslash does not escape the closing parenthesis
      "(() 1 beginbfrange ",                     // parentheses do not nest
      "{ 1 beginbfrange ",                       // an unmatched brace is an operator, not a container
      "} { 1 beginbfrange ",
      "1beginbfrange ",                          // the count ends at the first non-digit
      "/Name 12.5beginbfrange ",
      "endcodespacerange1beginbfrange ",         // an operator ends at the first digit
      "2 beginbfrange <00> <01> (x) "            // a string destination is skipped and the next range read
  })
  public void malformedCMapLayoutsStillCountTheRangeCodes(String prefix) throws Exception {
    String cmap = prefix + OVER_CAP_RANGE;
    assertNotNull(parseWithFontbox(cmap).toUnicode(LAST_CODE, 3), "fontbox expands the range of: " + cmap);
    StreamStructureScanner.Refusal failure =
        assertThrows(StreamStructureScanner.Refusal.class, () -> scan(cmap), cmap);
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "(\\) 1 beginbfrange ",
      "(() 1 beginbfrange ",
      "{ 1 beginbfrange ",
      "1beginbfrange ",
      "2 beginbfrange <00> <01> (x) "
  })
  public void malformedCMapLayoutsAtTheCapPass(String prefix) throws Exception {
    // 0x000003 to 0x040000 is two codes under the cap; the leading range of the last layout adds those two.
    String cmap = prefix + "<000003> <040000> <0041> endbfrange";
    scan(cmap);
  }

  private static CMap parseWithFontbox(String cmap) throws Exception {
    try (RandomAccessReadBuffer buffer = new RandomAccessReadBuffer(cmap.getBytes(StandardCharsets.ISO_8859_1))) {
      return new CMapParser(false).parse(buffer);
    }
  }

  private static void scan(String cmap) throws Exception {
    try (StreamStructureScanner.CMapRanges scanner = new StreamStructureScanner.CMapRanges("test CMap")) {
      scanner.write(cmap.getBytes(StandardCharsets.ISO_8859_1));
    }
  }
}
