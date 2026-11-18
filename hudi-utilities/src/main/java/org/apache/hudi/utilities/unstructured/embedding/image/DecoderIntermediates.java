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

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.plugins.tiff.BaselineTIFFTagSet;
import javax.imageio.plugins.tiff.TIFFDirectory;
import javax.imageio.plugins.tiff.TIFFField;
import javax.imageio.stream.ImageInputStream;

import java.io.EOFException;
import java.io.IOException;

/**
 * Bytes a JDK reader needs BEFORE source subsampling applies, estimated from the image's header fields without decoding a pixel
 * (measured on 50 MP fixtures: the strip of a single-strip TIFF lands on the heap and a progressive JPEG's
 * coefficients in native memory, each estimated at about 143 MiB, while the subsampled destination raster is 0.59 MB). Sizes
 * are the ones the header DECLARES: Deflate allocates the declared byte count before reading a byte, and the other readers
 * read up to it (at or above one 1,024,000-byte unit, the staggered read of LZW, PackBits, CCITT and BMP RLE allocates its units
 * as the data arrives, so a declared size beyond the data costs only what is read). For the terms below the estimate is an upper
 * bound of the reader's own arrays; it does not count a BMP's embedded JPEG/PNG (BI_JPEG/BI_PNG) or JPEG-in-TIFF tables:
 * <ul>
 *   <li>TIFF: the reader decodes a whole strip (or tile) at full resolution, then subsamples it: one strip/tile =
 *       {@code width x rowsPerStrip} (or {@code tileWidth x tileLength}) x bits per pixel / 8; a compressed strip/tile is
 *       read whole beside it, so the largest TileByteCounts (else StripByteCounts) entry is added, twice for the compressions
 *       whose decompressor reads it in units and then copies them into one array (LZW, PackBits, CCITT; below one unit the
 *       read is a single array, so 2 x over-counts by at most 1,024,000 B), once for the others (Deflate; JPEG and old JPEG,
 *       whose tables, typically under 1 KiB, are not counted);</li>
 *   <li>BMP BI_RLE8 / BI_RLE4: the reader reads the whole compressed bitmap in units and copies them into one array before
 *       decoding a row: 2 x the bitmap size, the info header's imageSize or, when that is 0, bfSize - bfOffBits;</li>
 *   <li>progressive JPEG (SOF2): libjpeg keeps every DCT coefficient of the image: per component
 *       {@code ceil(componentWidth / 8) x ceil(componentHeight / 8)} blocks of 64 two-byte coefficients (native memory);</li>
 *   <li>anything else: 0 (baseline JPEG, PNG, GIF and uncompressed BMP decode row by row into the subsampled
 *       destination).</li>
 * </ul>
 */
final class DecoderIntermediates {

  static final String TIFF_METADATA_FORMAT = "javax_imageio_tiff_image_1.0";
  static final String JPEG_METADATA_FORMAT = "javax_imageio_jpeg_image_1.0";
  /** SOF process of a progressive JPEG in the JDK's JPEG metadata. */
  private static final String PROGRESSIVE_PROCESS = "2";
  private static final int BLOCK = 8;
  private static final long BYTES_PER_BLOCK = 64L * 2;
  /** BMP header bytes read: the file header (14) and the info header up to its imageSize field. */
  private static final int BMP_HEADER_BYTES = 38;
  private static final int BMP_FILE_SIZE_OFFSET = 2;
  private static final int BMP_BITMAP_OFFSET_OFFSET = 10;
  private static final int BMP_INFO_HEADER_SIZE_OFFSET = 14;
  private static final int BMP_COMPRESSION_OFFSET = 30;
  private static final int BMP_IMAGE_SIZE_OFFSET = 34;
  /** Info header of OS/2 1.x / Windows 2.x: no compression field, so never RLE. */
  private static final long BMP_CORE_HEADER_SIZE = 12;
  private static final long BMP_BI_RLE8 = 1;
  private static final long BMP_BI_RLE4 = 2;
  /** The RLE bitmap is held twice while it is read: the units, then the one array they are copied into. */
  private static final long BMP_RLE_COPIES = 2;

  private DecoderIntermediates() {
  }

  /**
   * @param reader a JDK reader whose input is set (an {@link ImageInputStream}), positioned on image 0's header
   * @return the estimated bytes, 0 when the format decodes straight into the subsampled destination
   * @throws IOException when the metadata cannot be read (a corrupt header), or when the BMP header read fails in storage (a
   *     {@link BlobReadException} of compute's stored-input stream, retried by the caller)
   */
  static long estimate(ImageReader reader, ImageHeader header) throws IOException {
    if ("tiff".equals(header.getFormat())) {
      return tiffStripOrTile(reader.getImageMetadata(0), header);
    }
    if ("jpeg".equals(header.getFormat())) {
      return progressiveJpegCoefficients(reader.getImageMetadata(0), header);
    }
    if ("bmp".equals(header.getFormat())) {
      return bmpRleBitmap((ImageInputStream) reader.getInput());
    }
    return 0;
  }

  /**
   * Long.MAX_VALUE when the product overflows a long: the tile dimensions are 32-bit header values that the image's checked width
   * and height do not bound, and a wrapped product must reach the budget refusal, not slip under it.
   */
  private static long tiffStripOrTile(IIOMetadata metadata, ImageHeader header) throws IOException {
    TIFFDirectory directory = TIFFDirectory.createFromMetadata(metadata);
    long bitsPerPixel = bitsPerPixel(directory);
    TIFFField tileWidth = directory.getTIFFField(BaselineTIFFTagSet.TAG_TILE_WIDTH);
    TIFFField tileLength = directory.getTIFFField(BaselineTIFFTagSet.TAG_TILE_LENGTH);
    try {
      long pixels;
      if (tileWidth != null && tileLength != null) {
        pixels = Math.multiplyExact(tileWidth.getAsLong(0), tileLength.getAsLong(0));
      } else {
        TIFFField rowsPerStrip = directory.getTIFFField(BaselineTIFFTagSet.TAG_ROWS_PER_STRIP);
        long rows = rowsPerStrip == null ? header.getHeight() : Math.min(rowsPerStrip.getAsLong(0), header.getHeight());
        pixels = (long) header.getWidth() * rows;
      }
      long decoded = Math.addExact(Math.multiplyExact(pixels, bitsPerPixel), 7) / 8;
      return Math.addExact(decoded, Math.multiplyExact(compressedCopies(directory), largestByteCount(directory, decoded)));
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * How many times the decompressor holds a compressed strip/tile at once: 0 uncompressed (read straight into the decoded
   * buffer), 2 for the readers of the whole strip in units then one copy ({@code ReaderUtil.staggeredReadByteStream}: LZW,
   * PackBits, CCITT RLE / T.4 / T.6), 1 for every other compression (Deflate, JPEG, old JPEG and any other).
   */
  private static long compressedCopies(TIFFDirectory directory) {
    TIFFField field = directory.getTIFFField(BaselineTIFFTagSet.TAG_COMPRESSION);
    int compression = field == null ? BaselineTIFFTagSet.COMPRESSION_NONE : field.getAsInt(0);
    switch (compression) {
      case BaselineTIFFTagSet.COMPRESSION_NONE:
        return 0;
      case BaselineTIFFTagSet.COMPRESSION_LZW:
      case BaselineTIFFTagSet.COMPRESSION_PACKBITS:
      case BaselineTIFFTagSet.COMPRESSION_CCITT_RLE:
      case BaselineTIFFTagSet.COMPRESSION_CCITT_T_4:
      case BaselineTIFFTagSet.COMPRESSION_CCITT_T_6:
        return 2;
      default:
        return 1;
    }
  }

  /**
   * The largest TileByteCounts entry, else the largest StripByteCounts entry (the field the reader takes each strip/tile's
   * byte count from), as declared; {@code decoded} when the directory has neither (the reader then reads the uncompressed size).
   */
  private static long largestByteCount(TIFFDirectory directory, long decoded) {
    TIFFField counts = directory.getTIFFField(BaselineTIFFTagSet.TAG_TILE_BYTE_COUNTS);
    if (counts == null) {
      counts = directory.getTIFFField(BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS);
    }
    if (counts == null) {
      return decoded;
    }
    long largest = 0;
    for (int i = 0; i < counts.getCount(); i++) {
      largest = Math.max(largest, counts.getAsLong(i));
    }
    return largest;
  }

  /**
   * 2 x the bitmap of a BI_RLE8 / BI_RLE4 BMP, sized as the JDK reader sizes its whole-bitmap read: the info header's imageSize,
   * or bfSize - bfOffBits when it is 0; 0 for any other BMP. The header is read from the start of {@code in}, which is left
   * where it was ({@code BMPMetadata} does not carry imageSize).
   */
  private static long bmpRleBitmap(ImageInputStream in) throws IOException {
    byte[] header = new byte[BMP_HEADER_BYTES];
    in.mark();
    try {
      in.seek(0);
      in.readFully(header);
    } catch (EOFException e) {
      // shorter than a BITMAPINFOHEADER: an OS/2 1.x header or a truncated file, never RLE the reader would read
      return 0;
    } finally {
      in.reset();
    }
    long compression = le32(header, BMP_COMPRESSION_OFFSET);
    if (le32(header, BMP_INFO_HEADER_SIZE_OFFSET) == BMP_CORE_HEADER_SIZE
        || (compression != BMP_BI_RLE8 && compression != BMP_BI_RLE4)) {
      return 0;
    }
    long imageSize = le32(header, BMP_IMAGE_SIZE_OFFSET);
    long bitmap = imageSize != 0 ? imageSize : le32(header, BMP_FILE_SIZE_OFFSET) - le32(header, BMP_BITMAP_OFFSET_OFFSET);
    return BMP_RLE_COPIES * Math.max(0, bitmap);
  }

  /** The unsigned little-endian 32-bit value at {@code offset}. */
  private static long le32(byte[] bytes, int offset) {
    return (bytes[offset] & 0xffL) | (bytes[offset + 1] & 0xffL) << 8 | (bytes[offset + 2] & 0xffL) << 16
        | (bytes[offset + 3] & 0xffL) << 24;
  }

  private static long bitsPerPixel(TIFFDirectory directory) {
    TIFFField bits = directory.getTIFFField(BaselineTIFFTagSet.TAG_BITS_PER_SAMPLE);
    TIFFField samples = directory.getTIFFField(BaselineTIFFTagSet.TAG_SAMPLES_PER_PIXEL);
    long samplesPerPixel = samples == null ? 1 : samples.getAsLong(0);
    if (bits == null) {
      return samplesPerPixel;
    }
    if (bits.getCount() >= samplesPerPixel) {
      long sum = 0;
      for (int i = 0; i < samplesPerPixel; i++) {
        sum += bits.getAsLong(i);
      }
      return sum;
    }
    return bits.getAsLong(0) * samplesPerPixel;
  }

  private static long progressiveJpegCoefficients(IIOMetadata metadata, ImageHeader header) {
    Node sof = find(metadata.getAsTree(JPEG_METADATA_FORMAT), "sof");
    if (sof == null || !PROGRESSIVE_PROCESS.equals(attribute(sof, "process"))) {
      return 0;
    }
    int maxH = 1;
    int maxV = 1;
    for (Node component = sof.getFirstChild(); component != null; component = component.getNextSibling()) {
      maxH = Math.max(maxH, Integer.parseInt(attribute(component, "HsamplingFactor")));
      maxV = Math.max(maxV, Integer.parseInt(attribute(component, "VsamplingFactor")));
    }
    long blocks = 0;
    for (Node component = sof.getFirstChild(); component != null; component = component.getNextSibling()) {
      long width = ceilDiv((long) header.getWidth() * Integer.parseInt(attribute(component, "HsamplingFactor")), maxH);
      long height = ceilDiv((long) header.getHeight() * Integer.parseInt(attribute(component, "VsamplingFactor")), maxV);
      blocks += ceilDiv(width, BLOCK) * ceilDiv(height, BLOCK);
    }
    return blocks * BYTES_PER_BLOCK;
  }

  private static Node find(Node node, String name) {
    if (name.equals(node.getNodeName())) {
      return node;
    }
    for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
      Node found = find(child, name);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static String attribute(Node node, String name) {
    NamedNodeMap attributes = node.getAttributes();
    Node value = attributes == null ? null : attributes.getNamedItem(name);
    return value == null ? null : value.getNodeValue();
  }

  private static long ceilDiv(long value, long divisor) {
    return (value + divisor - 1) / divisor;
  }
}
