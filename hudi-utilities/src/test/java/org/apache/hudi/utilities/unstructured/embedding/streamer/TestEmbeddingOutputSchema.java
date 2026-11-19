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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** The schema of an embedding round's rows on an existing table: the table's own columns, never a column of an older layout. */
class TestEmbeddingOutputSchema {

  private static EmbeddingTableConfig prepOnly() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "file:///tmp/embedding_table");
    return EmbeddingTableConfig.from(props);
  }

  /** The round keeps an existing table's columns as they are: it adds no column the table lacks, prepared_image among them. */
  @Test
  void existingTableColumnsAreKeptAsTheyAre() {
    EmbeddingTableConfig config = prepOnly();
    StructType table = new StructType(Arrays.stream(EmbeddingTableSchema.build(config).fields())
        .filter(field -> !field.name().equals(EmbeddingTableSchema.PREPARED_IMAGE)).toArray(StructField[]::new));
    StructType round = EmbeddingTableTransformer.outputSchema(config, Option.of(table), Option.empty());
    assertArrayEquals(table.fieldNames(), round.fieldNames());
  }
}
