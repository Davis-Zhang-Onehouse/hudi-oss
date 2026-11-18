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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The start-up checks of the embedding table: whether it exists and its table version. */
public class TestEmbeddingTableChecks {

  @TempDir
  Path temp;

  private final StorageConfiguration<?> conf = HoodieTestUtils.getDefaultStorageConf();

  private static EmbeddingTableConfig config(String writeTableVersion) {
    TypedProperties props = new TypedProperties();
    if (writeTableVersion != null) {
      props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), writeTableVersion);
    }
    return EmbeddingTableConfig.from(props);
  }

  @Test
  public void writeTableVersionBelowNineIsRefused() {
    String table = temp.resolve("none").toString();
    assertThrows(EmbeddingTableVersionException.class, () -> EmbeddingTableChecks.checkTableVersion(table, conf, config("8")));
    assertThrows(EmbeddingTableVersionException.class, () -> EmbeddingTableChecks.checkTableVersion(table, conf, config("x")));
    EmbeddingTableChecks.checkTableVersion(table, conf, config(null));
  }

  @Test
  public void existingTableBelowNineIsRefused() throws Exception {
    String table = temp.resolve("v8").toString();
    HoodieTestUtils.init(table, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.EIGHT);
    EmbeddingTableVersionException error = assertThrows(EmbeddingTableVersionException.class,
        () -> EmbeddingTableChecks.checkTableVersion(table, conf, config(null)));
    assertTrue(error.getMessage().contains(HoodieTableConfig.VERSION.key() + "=8"), error.getMessage());
  }

  /** A table left with only the hoodie.properties backup, as a crash during a table config update leaves it, exists. */
  @Test
  public void tableWithOnlyThePropertiesBackupExists() throws Exception {
    String table = temp.resolve("backup").toString();
    HoodieTestUtils.init(conf, table);
    Path metaFolder = temp.resolve("backup").resolve(HoodieTableMetaClient.METAFOLDER_NAME);
    Files.move(metaFolder.resolve(HoodieTableConfig.HOODIE_PROPERTIES_FILE),
        metaFolder.resolve(HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP));
    assertTrue(EmbeddingTableChecks.tableExists(table, conf));
  }
}
