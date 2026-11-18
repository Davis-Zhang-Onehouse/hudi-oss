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

import java.io.Serializable;

/**
 * The counts of one {@link ProviderRetryLoop} call.
 */
public final class EmbeddingCallStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int requests;
  private final int retries;
  private final long rateLimitWaitMillis;

  /**
   * @param requests             requests sent, retries included
   * @param retries              requests repeated after a retryable failure
   * @param rateLimitWaitMillis  time the call waited for tokens of its {@link RequestRateLimiter}
   */
  public EmbeddingCallStats(int requests, int retries, long rateLimitWaitMillis) {
    this.requests = requests;
    this.retries = retries;
    this.rateLimitWaitMillis = rateLimitWaitMillis;
  }

  public int getRequests() {
    return requests;
  }

  public int getRetries() {
    return retries;
  }

  /** Time the call waited for rate-limit tokens before its requests; 0 when the model has no rate limit. */
  public long getRateLimitWaitMillis() {
    return rateLimitWaitMillis;
  }
}
