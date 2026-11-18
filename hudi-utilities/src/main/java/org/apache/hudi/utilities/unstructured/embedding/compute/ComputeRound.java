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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * The working error column of the compute step ({@link MicroBatchEmbeddingIterator}).
 */
public final class ComputeRound {

  /**
   * The working column {@link MicroBatchEmbeddingIterator} stores a PERMANENT outcome in. A reserved name outside the
   * model-column namespace.
   */
  public static final String ERROR_WORKING_COLUMN = "_embedding_compute_error";

  private ComputeRound() {
  }

  /** The nullable string field of {@link #ERROR_WORKING_COLUMN}. */
  public static StructField errorWorkingField() {
    return DataTypes.createStructField(ERROR_WORKING_COLUMN, DataTypes.StringType, true);
  }
}
