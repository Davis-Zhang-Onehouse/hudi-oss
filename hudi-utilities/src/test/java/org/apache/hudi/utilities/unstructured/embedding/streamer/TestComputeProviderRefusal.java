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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.ComputeRound;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A provider's permanent refusal ({@link EmbeddingClientConfigException}: a rejected credential, an exhausted quota) ends the
 * compute round at the first refusal: one request, no Spark task re-attempt, the round fails with the provider's message and
 * nothing is committed. A transient answer is still retried. Spark runs {@code local[1,4]} here: one compute thread, and a failed
 * task would get four attempts (as {@code spark.task.maxFailures=4}).
 */
@Tag("functional")
class TestComputeProviderRefusal extends EmbeddingRoundTestBase {

  private static final String QUOTA = "OpenAI HTTP 429 insufficient_quota: You exceeded your current quota";
  private static final int FILES = 3;

  @Override
  public SparkConf conf() {
    return conf(Collections.singletonMap("spark.master", "local[1,4]"));
  }

  @Test
  void permanentRefusalFailsTheRoundAfterOneRequestWithoutATaskReattempt() throws Exception {
    announce(files());
    ScriptedEmbeddingClient.REFUSAL.set(QUOTA);

    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "1",
        EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key(), "2");
    RuntimeException failure = assertThrows(RuntimeException.class, () -> round(props));

    EmbeddingClientConfigException refusal = refusal(failure);
    assertTrue(refusal.getMessage().startsWith(ComputeRound.REFUSED_PREFIX.trim() + " model=" + EmbeddingStreamers.MODEL + ": "),
        refusal.getMessage());
    assertTrue(refusal.getMessage().endsWith(": " + QUOTA), refusal.getMessage());
    assertEquals(1, ScriptedEmbeddingClient.CALLS.get(), "one request: the task stopped at the first refusal (9 rows, 5 batches)");
    assertEquals(0, ScriptedEmbeddingClient.REATTEMPT_CALLS.get(), "no Spark task re-attempt");
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "nothing was committed");
  }

  @Test
  void transientAnswerIsStillRetriedAndTheRoundCommits() throws Exception {
    announce(files());
    ScriptedEmbeddingClient.RETRYABLE.set(1);

    Round round = round(props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "1", EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key(), "2",
        EmbeddingTableConfig.COMPUTE_RETRY_BACKOFF_MS.key(), "0"));

    assertTrue(round.getCommit().isPresent(), round.toString());
    assertEquals(3 * FILES, rows(table()).size());
    rows(table()).values().forEach(row -> assertNotNull(row.getAs(EmbeddingStreamers.MODEL_COLUMN), row.toString()));
    assertEquals(5 + 2, ScriptedEmbeddingClient.CALLS.get(), "5 batches, plus the 2 inputs of the retryable batch alone");
  }

  private static FixtureFile[] files() {
    FixtureFile[] files = new FixtureFile[FILES];
    for (int i = 0; i < FILES; i++) {
      files[i] = text("f" + i, "one " + i, "two " + i, "three " + i);
    }
    return files;
  }

  private static EmbeddingClientConfigException refusal(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof EmbeddingClientConfigException) {
        return (EmbeddingClientConfigException) cause;
      }
    }
    throw new AssertionError("no EmbeddingClientConfigException in the round's failure", failure);
  }
}
