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

import java.util.List;
import java.util.Properties;

/** Resource-free preparation of a source's job before the streamer builds its consumers. */
public interface SourceContract {

  /** Returns the job's settings, rejecting conflicts without changing the caller's properties. */
  TypedProperties prepareJob(TypedProperties props);

  /** Whether these unchanged properties already passed this contract's job preparation. */
  default boolean isPreparedJob(TypedProperties props) {
    return false;
  }

  /** Opaque properties to persist when this source creates a new table. */
  default Properties tableProperties() {
    return new Properties();
  }

  /** Returns the transformers to run, including any the source requires. */
  default List<String> transformerChain(List<String> configured) {
    return configured;
  }
}
