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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Bounded copy settings, tested through the copyRow API. */
class TestBlobCopyRetryConfig {

  @TempDir
  Path temp;

  @ParameterizedTest
  @CsvSource({"copy.retry.max.attempts,0", "copy.retry.max.attempts,-1", "copy.retry.max.attempts,many",
      "copy.retry.max.attempts,2147483648", "copy.retry.backoff.ms,-1", "copy.retry.backoff.ms,soon",
      "copy.buffer.bytes,0", "copy.buffer.bytes,many"})
  void invalidCopySettingFailsWithTypedRefusalNamingTheKey(String suffix, String value) throws Exception {
    Path source = temp.resolve("a.pdf");
    Files.write(source, new byte[] {1, 2, 3});
    String path = source.toUri().toString();
    String sha = BlobFileNames.sourcePathSha(path);
    Row row = RowFactory.create(sha, 1234L, path, "a.pdf", "pdf", 3L, "application/pdf",
        RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null, RowFactory.create(null, 0L, 3L, true)));
    Properties props = new Properties();
    props.setProperty("hoodie.blob.table.copy.retry.max.attempts", "1");
    props.setProperty("hoodie.blob.table.copy.retry.backoff.ms", "0");
    String key = "hoodie.blob.table." + suffix;
    props.setProperty(key, value);
    String base = temp.resolve("table").toUri().toString();
    HoodieHadoopStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()));
    OutOfLineWriter writer = new OutOfLineWriter(storage, base, "20261030182900000", Option.empty());
    StructType schema = BlobTableSchema.schema();
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> BlobObjectCopier.copyRow(row, schema, props, "pdf", storage, writer));
    assertEquals("BlobSourceConfigException", failure.getClass().getSimpleName());
    assertTrue(failure.getMessage().contains(key), failure.getMessage());
    assertTrue(failure.getMessage().contains(value), failure.getMessage());
  }
}
