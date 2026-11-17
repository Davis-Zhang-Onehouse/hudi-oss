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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingCallStats;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderResponseKind;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop.Answer;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.RetryAfter;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockInvoker.BedrockReply;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * An {@link EmbeddingClient} over Amazon Bedrock InvokeModel for one text model family.
 * Requests use the AWS SDK default credential chain through {@link SdkBedrockInvoker}.
 * Texts are packed within the family's request limits. Empty or unsupported inputs fail permanently without a request.
 * {@link ProviderRetryLoop} bounds retries, rate waits and deadlines; malformed answers remain retryable.
 * Rejected packed inputs are isolated. Before any successful embedding, an entirely rejected multi-input call fails
 * configuration rather than quarantining rows; a rejection beside retryable inputs stays retryable.
 */
public abstract class BedrockEmbeddingClient implements EmbeddingClient {

  private static final Logger LOG = LoggerFactory.getLogger(BedrockEmbeddingClient.class);
  /** Provider id of the Bedrock clients ({@code hoodie.unstructured.embedding.model.<alias>.provider}). */
  public static final String PROVIDER = "bedrock";
  static final String EMPTY_INPUT = "empty input";
  static final String COHERE_INPUT_TYPE = "search_document";
  /** Cohere's {@code truncate}: an over-long input is rejected, never silently cut. */
  static final String COHERE_TRUNCATE = "NONE";
  /** Bedrock: the request's content is rejected (400). */
  private static final String BEDROCK_VALIDATION = "ValidationException";
  private static final Set<String> BEDROCK_RETRYABLE = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "ThrottlingException", "ModelNotReadyException", "ModelTimeoutException", "ModelErrorException",
      "ServiceUnavailableException", "InternalServerException", "ServiceQuotaExceededException")));
  /**
   * The AWS SDK's clock-skew codes (AwsErrorCode, SDK 2.29.52; RequestExpired "The request is no longer valid due to expired
   * timestamps.", https://docs.aws.amazon.com/bedrock/latest/userguide/troubleshooting-api-error-codes.html): transient, since the SDK
   * adjusts its clock offset on the failed response and the next attempt is signed again. One that persists leaves its inputs
   * RETRYABLE, so the run fails; it is never an input's rejection.
   */
  private static final Set<String> BEDROCK_CLOCK_SKEW = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "RequestTimeTooSkewed", "RequestExpired", "RequestInTheFuture")));
  /** A signature the SDK made with a skewed clock ({@code InvalidSignatureException} "Signature expired: ..."): a clock skew too. */
  private static final String BEDROCK_INVALID_SIGNATURE = "InvalidSignatureException";
  private static final String SIGNATURE_EXPIRED = "Signature expired";
  /**
   * The request was aborted before Bedrock answered ({@code RequestAbortedException} 400, "aborted before a response could be
   * returned", https://docs.aws.amazon.com/bedrock/latest/APIReference/CommonErrors.html): transient, never the input's.
   */
  private static final String BEDROCK_REQUEST_ABORTED = "RequestAbortedException";
  private static final Set<String> BEDROCK_CONFIG = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "AccessDeniedException", "ResourceNotFoundException", "UnrecognizedClientException", BEDROCK_INVALID_SIGNATURE,
      "ExpiredTokenException", "IncompleteSignature", "MissingAuthenticationToken", "NotAuthorized", "InvalidClientTokenId")));
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_ERROR_CHARS = 1024;
  /** Cohere caps a request at 128k tokens; a text counts one token per UTF-8 byte plus 8, an upper bound. */
  private static final long COHERE_MAX_REQUEST_TOKENS = 128_000L;
  private static final long COHERE_TOKENS_PER_INPUT = 8L;

  private final Function<BedrockClientConfig, BedrockInvoker> invokers;
  private final LongSupplier clock;
  private final ProviderRetryLoop.Sleeper sleeper;
  private final Random jitter;
  private EmbeddingModelSpec spec;
  private BedrockClientConfig config;
  private BedrockInvoker invoker;
  private ProviderRetryLoop loop;
  /** Set once any request of this client returned vectors: the binding's configuration works. */
  private boolean embeddedOnce;
  private EmbeddingCallStats lastCallStats = EmbeddingCallStats.NONE;

  /** Production wiring: the SDK invoker, the system clock, real sleeps. */
  protected BedrockEmbeddingClient() {
    this(SdkBedrockInvoker::create, System::currentTimeMillis, Thread::sleep, new Random());
  }

  /** Test seam: the invoker factory, the clock and sleeper of the retry loop and rate limiter, the backoff jitter. */
  BedrockEmbeddingClient(Function<BedrockClientConfig, BedrockInvoker> invokers, LongSupplier clock,
                         ProviderRetryLoop.Sleeper sleeper, Random jitter) {
    this.invokers = invokers;
    this.clock = clock;
    this.sleeper = sleeper;
    this.jitter = jitter;
  }

  /** The family this client serves. */
  public abstract BedrockModelFamily family();

  /**
   * @throws EmbeddingClientConfigException naming the offending key for an invalid configuration, or when the SDK client cannot
   *     be built (no region)
   */
  @Override
  public void init(EmbeddingModelSpec spec, TypedProperties props) {
    this.spec = spec;
    try {
      this.config = BedrockClientConfig.from(spec, props, family());
    } catch (IllegalArgumentException e) {
      throw new EmbeddingClientConfigException(e.getMessage());
    }
    RequestRateLimiter rateLimiter = RequestRateLimiter.from(spec, props, clock, sleeper::sleep);
    this.loop = new ProviderRetryLoop("model=" + spec.getName() + " provider=" + PROVIDER + " family=" + family(),
        config.settings(), rateLimiter, clock, sleeper, jitter);
    this.invoker = invokers.apply(config);
  }

  @Override
  public EmbeddingModelSpec spec() {
    return spec;
  }

  @Override
  public EmbeddingCallStats lastCallStats() {
    return lastCallStats;
  }

  BedrockClientConfig config() {
    return config;
  }

  @Override
  public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
    lastCallStats = EmbeddingCallStats.NONE;
    if (loop.outageOpen()) {
      throw new EmbeddingProviderUnavailableException(ProviderRetryLoop.OUTAGE + " is open for model " + spec.getName() + " ("
          + PROVIDER + " " + config.modelId() + ") after " + loop.consecutiveFailures()
          + " consecutive failed requests; the remaining rows of this task are left for a later run");
    }
    loop.startCall();
    Call call = new Call(inputs.size());
    try {
      List<Integer> texts = new ArrayList<>();
      for (int i = 0; i < inputs.size(); i++) {
        EmbeddingInput input = inputs.get(i);
        if (EmbeddingTableSchema.MODALITY_TEXT.equals(input.getModality())) {
          if (input.getText() == null || input.getText().trim().isEmpty()) {
            call.results[i] = EmbeddingResult.permanent(EMPTY_INPUT);
          } else {
            texts.add(i);
          }
        } else {
          call.results[i] = EmbeddingResult.permanent("modality " + input.getModality() + " is not supported by " + PROVIDER
              + " " + family());
        }
      }
      for (List<Integer> request : packTexts(inputs, texts)) {
        sendTexts(call, inputs, request);
      }
      settleRejections(call);
    } finally {
      // also when a config failure is thrown: the stats then describe the requests that led to it
      lastCallStats = loop.stats(call.isolationRequests, call.tokens);
    }
    return Arrays.asList(call.results);
  }

  /** Text requests: consecutive texts up to the per-request input limit and, for Cohere, its token budget. */
  private List<List<Integer>> packTexts(List<EmbeddingInput> inputs, List<Integer> texts) {
    int maxInputs = Math.min(family().maxTextsPerRequest(), config.settings().maxInputs());
    List<List<Integer>> requests = new ArrayList<>();
    List<Integer> current = new ArrayList<>();
    long tokens = 0;
    for (int index : texts) {
      long cost = inputs.get(index).getText().getBytes(StandardCharsets.UTF_8).length + COHERE_TOKENS_PER_INPUT;
      if (!current.isEmpty() && (current.size() >= maxInputs || tokens + cost > COHERE_MAX_REQUEST_TOKENS)) {
        requests.add(current);
        current = new ArrayList<>();
        tokens = 0;
      }
      current.add(index);
      tokens += cost;
    }
    if (!current.isEmpty()) {
      requests.add(current);
    }
    return requests;
  }

  private void sendTexts(Call call, List<EmbeddingInput> inputs, List<Integer> request) {
    String notSent = loop.notSentReason();
    if (notSent != null) {
      call.fill(request, EmbeddingResult.retryable(notSent));
      return;
    }
    List<String> texts = new ArrayList<>(request.size());
    for (int index : request) {
      texts.add(inputs.get(index).getText());
    }
    byte[] body = textBody(texts);
    Answer<float[][]> answer = loop.send(() -> attempt(call, body, texts.size()));
    switch (answer.kind()) {
      case OK:
        accept(call, request, answer.value());
        break;
      case INPUT:
        if (request.size() == 1) {
          call.rejected.put(request.get(0), answer.error());
        } else {
          // isolate the rejected inputs: each one alone
          for (int index : request) {
            call.isolationRequests++;
            sendTexts(call, inputs, Collections.singletonList(index));
          }
        }
        break;
      case RETRYABLE:
        call.fill(request, EmbeddingResult.retryable(answer.error()));
        break;
      default:
        throw configFailure(answer.error());
    }
  }

  /** One attempt: invoke, classify, parse; never throws for a provider or transport failure. */
  private Answer<float[][]> attempt(Call call, byte[] body, int expected) throws InterruptedException {
    BedrockReply reply;
    try {
      reply = invoker.invoke(config.modelId(), body);
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "Bedrock request failed: " + e.getMessage(), -1);
    }
    ProviderResponseKind kind = classifyBedrock(reply.status(), reply.errorCode(), reply.message());
    if (kind != ProviderResponseKind.OK) {
      String error = "Bedrock " + (reply.status() > 0 ? "HTTP " + reply.status() + " " : "") + reply.errorCode() + ": "
          + cut(reply.message());
      return Answer.failed(kind, error, RetryAfter.hintMillis(null, reply.retryAfter(), clock.getAsLong()));
    }
    try {
      float[][] vectors = parse(reply.body(), expected);
      long tokens = reply.inputTokens() >= 0 ? reply.inputTokens() : titanTokenCount(reply.body());
      if (tokens >= 0) {
        call.tokens = Math.max(call.tokens, 0) + tokens;
      }
      return Answer.ok(vectors);
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "malformed answer: " + e.getMessage(), -1);
    }
  }

  /**
   * Amazon Bedrock InvokeModel errors, by the error code the SDK reads from the response
   * (https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModel.html#API_runtime_InvokeModel_Errors):
   * throttling, a model not ready or timing out, a model error, server errors, a clock skew (an expired signature included) and an
   * aborted request are transient; access denied, a missing permission (NotAuthorized, a 400 on
   * https://docs.aws.amazon.com/bedrock/latest/userguide/troubleshooting-api-error-codes.html), an unknown model and a rejected or
   * expired credential reject the configuration; {@value #BEDROCK_VALIDATION} rejects an input. A code not
   * listed falls back to the status: 408, 429 and 5xx transient, 400/413/422 an input, anything else the configuration.
   *
   * @param status the HTTP status, or -1 when no response came back (the credentials could not be resolved)
   * @param errorCode the error code, or null
   * @param message the error message, or null
   */
  static ProviderResponseKind classifyBedrock(int status, String errorCode, String message) {
    if (status >= 200 && status < 300) {
      return ProviderResponseKind.OK;
    }
    if (errorCode != null) {
      if (BEDROCK_RETRYABLE.contains(errorCode) || BEDROCK_CLOCK_SKEW.contains(errorCode) || BEDROCK_REQUEST_ABORTED.equals(errorCode)
          || (BEDROCK_INVALID_SIGNATURE.equals(errorCode) && message != null && message.startsWith(SIGNATURE_EXPIRED))) {
        return ProviderResponseKind.RETRYABLE;
      }
      if (BEDROCK_CONFIG.contains(errorCode)) {
        return ProviderResponseKind.CONFIG;
      }
      if (BEDROCK_VALIDATION.equals(errorCode)) {
        return ProviderResponseKind.INPUT;
      }
    }
    if (status == 408 || status == 429 || status >= 500) {
      return ProviderResponseKind.RETRYABLE;
    }
    if (status == 400 || status == 413 || status == 422) {
      return ProviderResponseKind.INPUT;
    }
    return ProviderResponseKind.CONFIG;
  }

  /** OK vectors: a vector count other than the inputs' is malformed; a dimension other than the model's is configuration. */
  private void accept(Call call, List<Integer> request, float[][] vectors) {
    embeddedOnce = true;
    for (int i = 0; i < request.size(); i++) {
      if (vectors[i].length != spec.getDimension()) {
        throw configFailure("Bedrock returned " + vectors[i].length + "-dimension vectors, but the model's .dimensions is "
            + spec.getDimension());
      }
      call.results[request.get(i)] = EmbeddingResult.ok(vectors[i]);
    }
  }

  /**
   * Inputs Bedrock rejected alone become PERMANENT once this client has embedded anything, or when the call holds one input: one
   * rejection cannot tell the row's fault from the binding's, and failing the run would fail every later run on the same row.
   * Otherwise the call fails as configuration, since a wrong model id or binding is rejected the same way.
   */
  private void settleRejections(Call call) {
    if (call.rejected.isEmpty()) {
      return;
    }
    if (!embeddedOnce && Arrays.stream(call.results).anyMatch(r -> r != null && r.getStatus() == EmbeddingResult.Status.RETRYABLE_FAILURE)) {
      // other inputs were only held back (throttled, not sent): nothing shows the configuration is wrong yet, so try again later
      for (Map.Entry<Integer, String> rejected : call.rejected.entrySet()) {
        call.results[rejected.getKey()] = EmbeddingResult.retryable(rejected.getValue()
            + " (rejected before this client embedded anything; retried)");
      }
      return;
    }
    if (!embeddedOnce && call.results.length > 1) {
      throw configFailure(call.rejected.size() + " input(s) rejected and nothing embedded yet by this client, so the "
          + "binding's configuration (model id, family, dimensions) may be the cause; first rejection: "
          + call.rejected.values().iterator().next());
    }
    for (Map.Entry<Integer, String> rejected : call.rejected.entrySet()) {
      call.results[rejected.getKey()] = EmbeddingResult.permanent(rejected.getValue());
    }
    LOG.warn("EMBED-BEDROCK-INPUT-REJECTED model={} rejected={}: {}", spec.getName(), call.rejected.size(),
        call.rejected.values().iterator().next());
  }

  private EmbeddingClientConfigException configFailure(String error) {
    return new EmbeddingClientConfigException("Bedrock rejected the configuration of model " + spec.getName() + " ("
        + config.modelId() + ", " + family() + ", region " + (config.region() == null ? "(default chain)" : config.region())
        + "): " + error);
  }

  private byte[] textBody(List<String> texts) {
    ObjectNode body = MAPPER.createObjectNode();
    switch (family()) {
      case TITAN_EMBED_TEXT_V2:
        body.put("inputText", texts.get(0));
        body.put("dimensions", spec.getDimension());
        break;
      case COHERE_EMBED_V4:
        cohereBody(body).putArray("texts").addAll(textNodes(texts));
        break;
      default:
        throw new UnsupportedOperationException("Unhandled Bedrock model family " + family());
    }
    return bytes(body);
  }

  /** Cohere Embed v4: {@code truncate} NONE "returns an error if the input exceeds the limit"; the default is not documented. */
  private ObjectNode cohereBody(ObjectNode body) {
    body.put("input_type", COHERE_INPUT_TYPE);
    body.put("truncate", COHERE_TRUNCATE);
    body.putArray("embedding_types").add("float");
    body.put("output_dimension", spec.getDimension());
    return body;
  }

  private static ArrayNode textNodes(List<String> texts) {
    ArrayNode array = MAPPER.createArrayNode();
    texts.forEach(array::add);
    return array;
  }

  private static byte[] bytes(ObjectNode body) {
    try {
      return MAPPER.writeValueAsBytes(body);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot write a Bedrock request", e);
    }
  }

  /** Titan: {@code embedding}; Cohere: {@code embeddings.float} (or a bare {@code embeddings} list). */
  private float[][] parse(byte[] body, int expected) throws IOException {
    JsonNode root = MAPPER.readTree(body);
    if (root == null || !root.isObject()) {
      throw new IOException("the body is not a JSON object");
    }
    List<JsonNode> vectors = new ArrayList<>(expected);
    if (family() == BedrockModelFamily.COHERE_EMBED_V4) {
      JsonNode embeddings = root.path("embeddings");
      JsonNode floats = embeddings.isObject() ? embeddings.path("float") : embeddings;
      if (floats.isArray()) {
        floats.forEach(vectors::add);
      }
    } else {
      vectors.add(root.path("embedding"));
    }
    if (vectors.size() != expected) {
      throw new IOException("expected " + expected + " embeddings, got " + vectors.size());
    }
    float[][] out = new float[expected][];
    for (int i = 0; i < expected; i++) {
      JsonNode vector = vectors.get(i);
      if (!vector.isArray() || vector.size() == 0) {
        throw new IOException("embedding " + i + " is not a non-empty array");
      }
      out[i] = new float[vector.size()];
      for (int j = 0; j < out[i].length; j++) {
        if (!vector.get(j).isNumber()) {
          throw new IOException("embedding " + i + " holds a non-number");
        }
        out[i][j] = (float) vector.get(j).asDouble();
        if (!Float.isFinite(out[i][j])) {
          throw new IOException("embedding " + i + " holds a non-finite float");
        }
      }
    }
    return out;
  }

  /** Titan's {@code inputTextTokenCount}, -1 when absent or for Cohere. */
  private long titanTokenCount(byte[] body) {
    if (family() == BedrockModelFamily.COHERE_EMBED_V4) {
      return -1L;
    }
    try {
      JsonNode count = MAPPER.readTree(body).path("inputTextTokenCount");
      return count.isIntegralNumber() ? count.asLong() : -1L;
    } catch (IOException e) {
      return -1L;
    }
  }

  private static String cut(String text) {
    if (text == null) {
      return "";
    }
    return text.length() <= MAX_ERROR_CHARS ? text : text.substring(0, MAX_ERROR_CHARS) + "…";
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + config + "}";
  }

  /** The per-call state: results by input position, inputs rejected alone, isolation requests, reported input tokens. */
  private static final class Call {
    private final EmbeddingResult[] results;
    private final Map<Integer, String> rejected = new LinkedHashMap<>();
    private int isolationRequests;
    private long tokens = -1L;

    private Call(int size) {
      this.results = new EmbeddingResult[size];
    }

    private void fill(List<Integer> request, EmbeddingResult result) {
      for (int index : request) {
        results[index] = result;
      }
    }
  }
}
