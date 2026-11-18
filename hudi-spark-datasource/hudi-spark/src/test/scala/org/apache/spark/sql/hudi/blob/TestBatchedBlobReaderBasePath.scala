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
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions._

import java.io.File
import java.nio.file.Files
import java.util.Properties

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

  @Test
  def testVersionOneResolvesSchemeLessPathsRegardlessOfOwnership(): Unit = {
    for (managed <- Seq(false, true); path <- Seq("rel/key.pdf", "/rel/key.pdf")) {
      assertEquals(new StoragePath("/tmp/t/rel/key.pdf"),
        new StoragePath(BatchedBlobReader.resolveReferencePath(path, managed, Some("/tmp/t"), Some("1"))))
    }
    assertEquals("file:/abs/key.pdf",
      BatchedBlobReader.resolveReferencePath("file:/abs/key.pdf", managed = false, None, Some("1")))
    assertEquals("s3a://bucket/key.pdf",
      BatchedBlobReader.resolveReferencePath("s3a://bucket/key.pdf", managed = true, None, Some("1")))
  }

  @Test
  def testVersionOneRequiresTheTableBaseAndRejectsEscapingPaths(): Unit = {
    for (managed <- Seq(false, true)) {
      assertThrows(classOf[IllegalArgumentException],
        () => BatchedBlobReader.resolveReferencePath("rel/key.pdf", managed, None, Some("1")))
      for (path <- Seq("../t2/key.pdf", ".hoodie/../../other/key.pdf")) {
        assertThrows(classOf[IllegalArgumentException],
          () => BatchedBlobReader.resolveReferencePath(path, managed, Some("/tmp/t"), Some("1")))
      }
    }
  }

  @Test
  def testUnknownOrMixedLayoutsRejectSchemeLessPaths(): Unit = {
    for (version <- Seq("2", "mixed"); path <- Seq("rel/key.pdf", "/abs/key.pdf")) {
      assertThrows(classOf[IllegalArgumentException],
        () => BatchedBlobReader.resolveReferencePath(path, managed = false, Some("/tmp/t"), Some(version)))
    }
  }

  /** Two relative objects in one lookahead batch keep their own bytes: ranges group on the resolved path. */
  @Test
  def testReadBatchedResolvesAgainstExplicitTableBasePath(): Unit = {
    assertReadBatchedResolvesAgainstTableBase(None, managed = true)
  }

  @Test
  def testReadBatchedResolvesVersionedUnmanagedPaths(): Unit = {
    assertReadBatchedResolvesAgainstTableBase(Some("1"), managed = false)
  }

  private def assertReadBatchedResolvesAgainstTableBase(layoutVersion: Option[String], managed: Boolean): Unit = {
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
      Row(HoodieSchema.Blob.OUT_OF_LINE, null, Row(s".hoodie/.blob/pdf/$name", 0L, 500L, managed))
    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(Row("a", ref("a.pdf")), Row("b", ref("b.pdf"))), 1), schema)

    val result = BatchedBlobReader.readBatched(df,
      new HadoopStorageConfiguration(sparkSession.sparkContext.hadoopConfiguration),
      columnName = Some("blob"), tableBasePath = Some(base.getAbsolutePath), tableLayoutVersion = layoutVersion)
      .collect().map(r => r.getString(0) -> r.getAs[Array[Byte]]("blob")).toMap

    assertEquals(Set("a", "b"), result.keySet)
    assertArrayEquals(first, result("a"))
    assertArrayEquals(second, result("b"))
  }

  @Test
  def testSqlRetainsLegacyAbsolutePaths(): Unit = {
    assertSqlPathResolution(versioned = false)
  }

  @Test
  def testRuleRewritesUnanalyzedProjection(): Unit = {
    assertRuleRewritesProjection(analyzed = false)
  }

  @Test
  def testRuleRewritesAlreadyAnalyzedProjection(): Unit = {
    assertRuleRewritesProjection(analyzed = true)
  }

  private def assertRuleRewritesProjection(analyzed: Boolean): Unit = {
    val blob = AttributeReference("blob", BlobType().asInstanceOf[StructType])()
    val plan = Project(Seq(Alias(ReadBlobExpression(blob), "data")()), LocalRelation(blob))
    if (analyzed) {
      plan.setAnalyzed()
    }
    val rewritten = ReadBlobRule(sparkSession).apply(plan)
    assertTrue(rewritten.exists(_.isInstanceOf[BatchedBlobRead]), s"analyzed=$analyzed")
    assertFalse(rewritten.expressions.exists(_.exists(_.isInstanceOf[ReadBlobExpression])))
    assertEquals(Seq(BinaryType), rewritten.output.map(_.dataType))
  }

  @Test
  def testSqlResolvesPathsUsingTheProducingTableVersion(): Unit = {
    assertSqlPathResolution(versioned = true)
  }

  private def assertSqlPathResolution(versioned: Boolean): Unit = {
    val conf = new HadoopStorageConfiguration(sparkSession.sparkContext.hadoopConfiguration)
    val table = new File(tempDir.toFile, s"layout_$versioned")
    val external = new File(tempDir.toFile, s"external_$versioned.bin")
    val payload = Array[Byte](11, 23, 47)
    Files.write(external.toPath, if (versioned) Array[Byte](99) else payload)
    val reference = external.getAbsolutePath
    val blob = Row(HoodieSchema.Blob.OUT_OF_LINE, null, Row(reference, null, null, false))
    val schema = StructType(Seq(
      StructField("fid", StringType, nullable = false),
      StructField("blob", BlobType().asInstanceOf[StructType], nullable = true, blobMetadata)))
    val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(Seq(Row("a", blob)), 1), schema)
    df.write.format("hudi")
      .option("hoodie.table.name", s"layout_$versioned")
      .option("hoodie.datasource.write.recordkey.field", "fid")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite").save(table.getAbsolutePath)
    if (versioned) {
      val stored = new File(table, reference.stripPrefix("/"))
      stored.getParentFile.mkdirs()
      Files.write(stored.toPath, payload)
      val meta = HoodieTableMetaClient.builder().setConf(conf).setBasePath(table.getAbsolutePath).build()
      val properties = new Properties()
      properties.setProperty("hoodie.table.blob.layout.version", "1")
      HoodieTableConfig.update(meta.getStorage, meta.getMetaPath, properties)
    }
    val query = sparkSession.read.format("hudi").load(table.getAbsolutePath)
      .selectExpr("fid", "read_blob(blob) AS data")
    val rows = query.collect()
    assertEquals(1, rows.length)
    assertArrayEquals(payload, rows.head.getAs[Array[Byte]]("data"), s"versioned=$versioned")
    if (versioned) {
      val union = sparkSession.read.format("hudi").load(table.getAbsolutePath).select("fid", "blob").unionByName(df)
      val resolvedBranches = union.selectExpr("fid", "read_blob(blob) AS data").collect()
      assertEquals(Seq(Seq[Byte](99), Seq[Byte](11, 23, 47)),
        resolvedBranches.map(_.getAs[Array[Byte]]("data").toSeq).sortBy(_.length).toSeq)
      val ambiguous = union.selectExpr("fid", "rand() AS nonce", "read_blob(blob) AS data")
      val error = assertThrows(classOf[Exception], () => ambiguous.collect())
      assertTrue(error.toString.contains("ambiguous blob layout version"), error.toString)
    }
  }
}
