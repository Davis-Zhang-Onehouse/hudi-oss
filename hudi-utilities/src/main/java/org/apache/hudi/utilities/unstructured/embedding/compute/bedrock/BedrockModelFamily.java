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

package org.apache.hudi.utilities.unstructured.embedding.compute.bedrock;

import org.apache.hudi.common.util.Option;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The Amazon Bedrock embedding model families the compute step embeds with: each has its own InvokeModel request and response
 * shape, output dimensions and per-request limits, and its own client class, so {@code .client.class}
 * names the family even when the model id is an ARN that does not.
 * <ul>
 *   <li>{@link #TITAN_EMBED_TEXT_V2}: {@code amazon.titan-embed-text-v2}, text only; {@code {"inputText","dimensions"}} →
 *       {@code embedding}; 256, 512 or 1024 dimensions; one input per request
 *       (https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-embed-text.html);</li>
 *   <li>{@link #COHERE_EMBED_V4}: {@code cohere.embed-v4}, text embeddings;
 *       {@code {"input_type":"search_document","truncate":"NONE","embedding_types":["float"],"output_dimension","texts"}} →
 *       {@code embeddings.float}; 256, 512, 1024 or 1536; up to 96 texts per request
 *       (https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-embed-v4.html).</li>
 * </ul>
 */
public enum BedrockModelFamily {
  TITAN_EMBED_TEXT_V2("amazon.titan-embed-text-v2", Arrays.asList(256, 512, 1024), 1),
  COHERE_EMBED_V4("cohere.embed-v4", Arrays.asList(256, 512, 1024, 1536), 96);

  private final String baseModelId;
  private final List<Integer> dimensions;
  private final int maxTextsPerRequest;

  BedrockModelFamily(String baseModelId, List<Integer> dimensions, int maxTextsPerRequest) {
    this.baseModelId = baseModelId;
    this.dimensions = Collections.unmodifiableList(dimensions);
    this.maxTextsPerRequest = maxTextsPerRequest;
  }

  /** The foundation model id without its version suffix, e.g. {@code cohere.embed-v4}. */
  public String baseModelId() {
    return baseModelId;
  }

  /** The output dimensions the family accepts. */
  public List<Integer> dimensions() {
    return dimensions;
  }

  /** Most texts one request carries. */
  public int maxTextsPerRequest() {
    return maxTextsPerRequest;
  }

  /**
   * The family a model id names: a foundation model id ({@code cohere.embed-v4:0}), a cross-region inference profile id
   * ({@code us.cohere.embed-v4:0}) or either one's ARN. Empty for an id that names no supported family, such as an application
   * inference profile or provisioned-throughput ARN, whose client is chosen with {@code .client.class}.
   */
  public static Option<BedrockModelFamily> fromModelId(String modelId) {
    if (modelId == null) {
      return Option.empty();
    }
    String id = modelId.trim();
    id = id.substring(id.lastIndexOf('/') + 1);
    int version = id.indexOf(':');
    String base = version < 0 ? id : id.substring(0, version);
    for (BedrockModelFamily family : values()) {
      if (base.equals(family.baseModelId) || base.endsWith("." + family.baseModelId)) {
        return Option.of(family);
      }
    }
    return Option.empty();
  }
}
