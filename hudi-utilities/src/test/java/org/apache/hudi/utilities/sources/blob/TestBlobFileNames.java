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

package org.apache.hudi.utilities.sources.blob;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBlobFileNames {

  @Test
  public void fidIsDeterministic32Hex() throws Exception {
    String path = "file:///tmp/data/a.pdf";
    String fid = BlobFileNames.sourcePathSha(path);
    assertEquals(fid, BlobFileNames.sourcePathSha(path));
    assertTrue(fid.matches("[0-9a-f]{32}"), fid);
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(path.getBytes(StandardCharsets.UTF_8));
    StringBuilder hex = new StringBuilder();
    for (byte b : digest) {
      hex.append(String.format("%02x", b));
    }
    assertEquals(hex.substring(0, 32), fid);
    assertTrue(!fid.equals(BlobFileNames.sourcePathSha("file:///tmp/data/b.pdf")));
  }

  @Test
  public void canonicalPathOfLocalFile() {
    assertEquals("file:///tmp/a.pdf", BlobTestPaths.canonicalPath(new File("/tmp/x/../a.pdf").toURI()));
    assertEquals("file:///tmp/a.pdf", BlobTestPaths.canonicalPath(URI.create("file:/tmp/a.pdf")));
  }

  @Test
  public void canonicalPathOfS3() {
    assertEquals("s3a://bkt/a/b.pdf", BlobTestPaths.canonicalPath(URI.create("s3a://bkt/a//b.pdf")));
  }

  @Test
  public void canonicalPathIsDecoded() {
    // source_path is the decoded form Hadoop's Path prints and StoragePath(String) opens; file_name is its last segment
    String canonical = BlobTestPaths.canonicalPath(new File("/tmp/my scan%.pdf").toURI());
    assertEquals("file:///tmp/my scan%.pdf", canonical);
    assertEquals("my scan%.pdf", BlobFileNames.fileName(canonical));
    assertEquals("file:///tmp/my scan%.pdf", BlobTestPaths.canonicalPath(URI.create("file:/tmp/my%20scan%25.pdf")));
  }

  @Test
  public void s3SchemesShareOneCanonicalForm() {
    assertEquals("s3a://bkt/k/a.pdf", BlobTestPaths.canonicalPath(URI.create("s3://bkt/k/a.pdf")));
    assertEquals("s3a://bkt/k/a.pdf", BlobTestPaths.canonicalPath(URI.create("s3n://bkt/k/a.pdf")));
    assertEquals(BlobFileNames.sourcePathSha(BlobTestPaths.canonicalPath(URI.create("s3://bkt/k/a.pdf"))),
        BlobFileNames.sourcePathSha(BlobTestPaths.canonicalPath(URI.create("s3a://bkt/k/a.pdf"))));
  }

  @Test
  public void opaqueOrRelativeUriIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> BlobTestPaths.canonicalPath(URI.create("mailto:a@b.c")));
    assertThrows(IllegalArgumentException.class, () -> BlobTestPaths.canonicalPath(URI.create("docs/a.pdf")));
  }

  @Test
  public void extensionIsLowercased() {
    assertEquals("pdf", BlobFileNames.extension("SCAN.PDF"));
    assertEquals("gz", BlobFileNames.extension("a.tar.gz"));
    assertEquals("", BlobFileNames.extension("README"));
    assertEquals("b.pdf", BlobFileNames.fileName("file:///tmp/a/b.pdf"));
  }

  @Test
  public void contentTypeOfPdf() {
    assertEquals("application/pdf", BlobFileNames.contentType("pdf"));
  }

  @Test
  public void contentTypesOfImages() {
    assertEquals("image/png", BlobFileNames.contentType("png"));
    assertEquals("image/jpeg", BlobFileNames.contentType("jpg"));
    assertEquals("image/jpeg", BlobFileNames.contentType("jpeg"));
    assertEquals("image/gif", BlobFileNames.contentType("gif"));
    assertEquals("image/bmp", BlobFileNames.contentType("bmp"));
    assertEquals("image/tiff", BlobFileNames.contentType("tif"));
    assertEquals("image/tiff", BlobFileNames.contentType("tiff"));
    assertEquals("image/webp", BlobFileNames.contentType("webp"));
    assertNull(BlobFileNames.contentType("mp4"), "video types are not named here");
  }

}
