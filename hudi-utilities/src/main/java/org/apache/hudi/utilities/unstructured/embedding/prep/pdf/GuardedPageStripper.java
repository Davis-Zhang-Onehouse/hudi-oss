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

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.IOException;
import java.io.Writer;

/** Extracts one PDF page in content-stream order with the configured text separators; it applies no limit of its own. */
final class GuardedPageStripper extends PDFTextStripper {
  GuardedPageStripper() {
    setSortByPosition(false);
    setShouldSeparateByBeads(false);
    setSuppressDuplicateOverlappingText(false);
    setParagraphEnd("\n\n");
    setLineSeparator("\n");
    setPageEnd("");
  }

  /** Extracts {@code page} of {@code document} into {@code out}; {@code processPage} is public. */
  void extract(PDDocument document, PDPage page, Writer out) throws IOException {
    this.document = document;
    this.output = out;
    processPage(page);
  }

}
