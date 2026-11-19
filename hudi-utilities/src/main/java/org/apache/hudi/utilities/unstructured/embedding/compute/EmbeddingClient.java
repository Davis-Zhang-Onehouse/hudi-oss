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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;

import java.util.List;

/**
 * Computes embeddings by calling a third-party endpoint ("model embedding computation is always 3 party
 * service endpoint call"). One micro batch per {@link #embed} call.
 *
 * <p>Lifecycle per Spark task: built by {@link EmbeddingClientFactory} (no-arg constructor, then {@link #init} once) →
 * {@link #embed} per micro batch → {@link #close}.
 *
 * <p>Outcome contract: {@link #embed} returns exactly one {@link EmbeddingResult} per input, in input order, each OK
 * (with a vector of {@code spec().getDimension()} floats), RETRYABLE_FAILURE or PERMANENT_FAILURE. A thrown exception
 * means every input of that call is retryable; the caller then tries each input of the call alone, so one bad input does
 * not fail its batch-mates, and keeps trying a retryable input alone, with backoff, until it has been tried
 * {@code compute.max.attempts} times. An answer of the wrong size, or an OK result without a vector
 * of the right dimension, makes every input of that call retryable ("malformed answer"). Two exceptions are not retried:
 * {@link EmbeddingClientConfigException} (the endpoint rejects the configuration, e.g. HTTP 401) and
 * {@link EmbeddingProviderUnavailableException} (the client's outage short-circuit is open, or its credential's token endpoint
 * cannot be reached) fail the task, so the compute
 * commit writes nothing and the next sync retries its rows.
 */
public interface EmbeddingClient extends AutoCloseable {

  /** Prepares the client for {@code spec}; called once, before any {@link #embed}. */
  void init(EmbeddingModelSpec spec, TypedProperties props);

  /** The model this client was initialized for. */
  EmbeddingModelSpec spec();

  /**
   * @return one result per input, in input order
   * @throws Exception any failure of the call as a whole; every input of the call is then retryable
   */
  List<EmbeddingResult> embed(List<EmbeddingInput> inputs) throws Exception;

  /**
   * What the last {@link #embed} call cost at the provider (requests, retries, bisection requests, reported tokens,
   * wall time, rate-limit wait); {@link EmbeddingCallStats#NONE} for a client that reports nothing. The compute step reads it
   * after every call, also after one that threw, so a client that reports stats resets them when {@code embed} starts and sets
   * them when it ends, by return or by exception.
   */
  default EmbeddingCallStats lastCallStats() {
    return EmbeddingCallStats.NONE;
  }

  /**
   * Whether the compute step takes inputs of several files rejected alike as each input's own rejection, never as the
   * request's. A per-provider decision, not a description of how requests are sent: only the Vertex client answers true;
   * every other client keeps the whole-call rule, also one that sends each input alone (Bedrock
   * Titan text, {@code .request.max.inputs=1}). False by default.
   */
  default boolean answersEachInputAlone() {
    return false;
  }

  @Override
  default void close() {
  }
}
