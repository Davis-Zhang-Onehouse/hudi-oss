/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.blob.BlobTestHelpers.blobMetadata
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions._

import java.io.File
import java.nio.file.Files

/**
 * The resolution rule for blob reference paths and the DataFrame API entry point that
 * takes an explicit table base path.
 */
@Tag("functional")
class TestBatchedBlobReaderBasePath extends HoodieClientTestBase {

  private val rel = ".hoodie/.blob/pdf/f.pdf"

  private def assertResolves(expected: String, externalPath: String, managed: Boolean, base: Option[String]): Unit =
    assertEquals(new StoragePath(expected),
      new StoragePath(BatchedBlobReader.resolveReferencePath(externalPath, managed, base)))

  @Test
  def testAbsoluteReferencesAreOpenedAsWritten(): Unit = {
    assertResolves("s3a://bucket/key.pdf", "s3a://bucket/key.pdf", managed = true, Some("/tmp/t"))
    assertResolves("/abs/key.pdf", "/abs/key.pdf", managed = true, Some("/tmp/t"))
    assertResolves("file:/abs/key.pdf", "file:/abs/key.pdf", managed = false, None)
  }

  @Test
  def testUnmanagedRelativeReferenceIsOpenedAsWritten(): Unit = {
    assertEquals("rel/key.pdf", BatchedBlobReader.resolveReferencePath("rel/key.pdf", managed = false, Some("/tmp/t")))
    assertEquals("rel/key.pdf", BatchedBlobReader.resolveReferencePath("rel/key.pdf", managed = false, None))
  }

  @Test
  def testManagedRelativeReferenceResolvesAgainstTableBasePath(): Unit = {
    assertResolves(s"/tmp/t/$rel", rel, managed = true, Some("/tmp/t"))
    assertResolves(s"/tmp/t/$rel", rel, managed = true, Some("/tmp/t/"))
    assertResolves(s"file:/tmp/t/$rel", rel, managed = true, Some("file:///tmp/t"))
    assertResolves(s"s3a://bucket/tables/t/$rel", rel, managed = true, Some("s3a://bucket/tables/t"))
  }

  @Test
  def testManagedRelativeReferenceEscapingTableBasePathIsRejected(): Unit = {
    val escaping = ".hoodie/../../other/key.pdf"
    val error = assertThrows(classOf[IllegalArgumentException],
      () => BatchedBlobReader.resolveReferencePath(escaping, managed = true, Some("/tmp/t")))
    assertTrue(error.getMessage.contains(escaping), error.getMessage)
    // A sibling directory sharing the base path as a string prefix is outside the table too.
    assertThrows(classOf[IllegalArgumentException],
      () => BatchedBlobReader.resolveReferencePath("../t2/key.pdf", managed = true, Some("/tmp/t")))
  }

  @Test
  def testManagedRelativeReferenceWithoutTableBasePathIsRejected(): Unit = {
    val error = assertThrows(classOf[IllegalArgumentException],
      () => BatchedBlobReader.resolveReferencePath(rel, managed = true, None))
    assertTrue(error.getMessage.contains(rel), error.getMessage)
  }

  /** Two relative objects in one lookahead batch keep their own bytes: ranges group on the resolved path. */
  @Test
  def testReadBatchedResolvesAgainstExplicitTableBasePath(): Unit = {
    val base = new File(tempDir.toFile, "explicit_base")
    val first = Array.tabulate[Byte](500)(i => (i % 7).toByte)
    val second = Array.tabulate[Byte](500)(i => (i % 11 + 100).toByte)
    Seq("a.pdf" -> first, "b.pdf" -> second).foreach { case (name, bytes) =>
      val f = new File(base, s".hoodie/.blob/pdf/$name")
      f.getParentFile.mkdirs()
      Files.write(f.toPath, bytes)
    }
    val schema = StructType(Seq(
      StructField("fid", StringType, nullable = false),
      StructField("blob", BlobType().asInstanceOf[StructType], nullable = true, blobMetadata)))
    def ref(name: String): Row =
      Row(HoodieSchema.Blob.OUT_OF_LINE, null, Row(s".hoodie/.blob/pdf/$name", 0L, 500L, true))
    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(Row("a", ref("a.pdf")), Row("b", ref("b.pdf"))), 1), schema)

    val result = BatchedBlobReader.readBatched(df,
      new HadoopStorageConfiguration(sparkSession.sparkContext.hadoopConfiguration),
      columnName = Some("blob"), tableBasePath = Some(base.getAbsolutePath))
      .collect().map(r => r.getString(0) -> r.getAs[Array[Byte]]("blob")).toMap

    assertArrayEquals(first, result("a"))
    assertArrayEquals(second, result("b"))
  }
}
