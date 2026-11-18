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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** The source hooks supported by the streamer; registering a source also covers its subclasses. */
public final class SourceContracts {

  public static final SourceContract NONE = props -> props;

  private static final Map<Class<?>, Function<TypedProperties, SourceContract>> FACTORIES =
      Collections.singletonMap(UnstructuredDataSource.class, UnstructuredSourceContract::new);

  private SourceContracts() {
  }

  /** Resolves a registered source without initializing it; normal source loading reports unknown classes. */
  public static SourceContract of(String sourceClassName, TypedProperties props) {
    if (StringUtils.isNullOrEmpty(sourceClassName)) {
      return NONE;
    }
    Class<?> sourceClass;
    try {
      sourceClass = Class.forName(sourceClassName.trim(), false, SourceContracts.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      return NONE;
    }
    for (Class<?> type = sourceClass; type != null; type = type.getSuperclass()) {
      Function<TypedProperties, SourceContract> factory = FACTORIES.get(type);
      if (factory != null) {
        return factory.apply(props);
      }
    }
    return NONE;
  }

  /** Dispatches source-owned table validation before the streamer's stock table validation. */
  public static void validate(SourceContract contract, String basePath, HoodieTableConfig table) {
    if (contract instanceof UnstructuredSourceContract) {
      UnstructuredDataSource.validate(((UnstructuredSourceContract) contract).mode(), basePath, table);
    }
  }

  /** Dispatches source-owned planning before constructing a source or reading any input. */
  public static void validate(SourceContract contract, JavaSparkContext jsc, SparkSession spark, List<String> chain,
                              TypedProperties props, Option<HoodieSchema> targetSchema, Option<StructType> tableSchema) {
    if (contract instanceof UnstructuredSourceContract) {
      UnstructuredDataSource.validate(((UnstructuredSourceContract) contract).mode(), jsc, spark, chain, props, targetSchema, tableSchema);
    }
  }
}
