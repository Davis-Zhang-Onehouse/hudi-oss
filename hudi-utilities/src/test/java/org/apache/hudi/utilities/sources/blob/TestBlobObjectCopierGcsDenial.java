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

package org.apache.hudi.utilities.sources.blob;

import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.googleapis.json.GoogleJsonResponseException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.AccessDeniedException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * How a copy failure of a GCS object is classified. The GCS connector reports a refused request (403) as an IOException caused by
 * its JSON error response, not as the {@link AccessDeniedException} S3A raises; both are an access denial, a platform fault.
 */
public class TestBlobObjectCopierGcsDenial {

  private static final String SOURCE = "gs://landing/docs/a.pdf";

  private static BlobCopyFailure.Fault classify(IOException cause) {
    return new BlobObjectCopier.Copy("sha", SOURCE, 10, "pdf").failure("open", cause, null, null).getFault();
  }

  /** The connector's wording: the request error wrapped once, the JSON response as its cause. */
  private static IOException gcs(int statusCode, String statusMessage) {
    GoogleJsonResponseException response = new GoogleJsonResponseException(statusCode, statusMessage,
        "GET https://storage.googleapis.com/storage/v1/b/landing/o/docs%2Fa.pdf\n{\"code\": " + statusCode + "}");
    return new IOException("Error accessing " + SOURCE, response);
  }

  /** A 403 whose JSON error names {@code reason}, as the connector reports a rate limit: a quota, not a denial. */
  private static IOException gcs403(String reason) {
    GoogleJsonResponseException response = new GoogleJsonResponseException(403, "Forbidden",
        "GET https://storage.googleapis.com/storage/v1/b/landing/o/docs%2Fa.pdf\n{\"code\": 403, \"errors\": [{\"domain\": "
            + "\"usageLimits\", \"reason\": \"" + reason + "\"}]}");
    return new IOException("Error accessing " + SOURCE, response);
  }

  @Test
  void gcsConnector403IsPlatformFault() {
    assertEquals(BlobCopyFailure.Fault.PLATFORM, classify(new AccessDeniedException(SOURCE, null, "s3a 403")), "the S3A denial");
    assertEquals(BlobCopyFailure.Fault.PLATFORM, classify(gcs(403, "Forbidden")), "the GCS connector's denial");
    assertEquals(BlobCopyFailure.Fault.PLATFORM, classify(new IOException("outer", gcs(403, "Forbidden"))), "a wrapped one");
  }

  @Test
  void otherGcsResponsesStayTransient() {
    assertEquals(BlobCopyFailure.Fault.TRANSIENT, classify(gcs(429, "Too Many Requests")));
    assertEquals(BlobCopyFailure.Fault.TRANSIENT, classify(gcs(503, "Service Unavailable")));
    assertEquals(BlobCopyFailure.Fault.TRANSIENT, classify(new IOException("403 Forbidden, but no HTTP response")));
  }

  @Test
  void gcsRateLimit403IsTransient() {
    assertEquals(BlobCopyFailure.Fault.TRANSIENT, classify(gcs403("rateLimitExceeded")), "rateLimitExceeded");
    assertEquals(BlobCopyFailure.Fault.TRANSIENT, classify(gcs403("userRateLimitExceeded")), "userRateLimitExceeded");
    assertEquals(BlobCopyFailure.Fault.PLATFORM, classify(gcs403("forbidden")), "a 403 naming another reason");
  }
}
