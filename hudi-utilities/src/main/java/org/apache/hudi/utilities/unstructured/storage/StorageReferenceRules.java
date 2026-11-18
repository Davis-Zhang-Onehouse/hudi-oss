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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StorageSchemes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The rules that tell a storage reference's own fault from the platform's, used by the blob reader
 * ({@code BlobBytesSource}). A caller keeps its own classification precedence and exception mapping; these rules only answer:
 * is a scheme served, and is a construction failure the reference's own (an invalid authority, a missing required authority, an
 * out-of-range port).
 */
public final class StorageReferenceRules {

  /**
   * Schemes whose authority names the cluster, bucket or container a path lives in, so a path without one names no file: HDFS,
   * S3 (s3a, s3), GCS (gs) and Azure (wasb, wasbs, abfs, abfss). Schemes that may go without one (file, viewfs, ...) are not here.
   */
  private static final Set<String> AUTHORITY_REQUIRED_SCHEMES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("hdfs", "s3a", "s3", "gs", "wasb", "wasbs", "abfs", "abfss")));

  /** The largest TCP port; a reference naming a larger one is malformed. */
  private static final int MAX_PORT = 65535;

  private StorageReferenceRules() {
  }

  /**
   * Whether {@code scheme} is one this deployment legitimately serves: a scheme Hudi supports
   * ({@link StorageSchemes#isSchemeSupported}), or any scheme with an {@code fs.<scheme>.impl} in the job's Hadoop configuration.
   * A file system missing for such a scheme is a platform failure (a connector absent on the executor); for any other scheme the
   * reference is malformed. An unreadable job configuration counts as served: it is not proof that the scheme is foreign.
   */
  public static boolean isServedScheme(String scheme, Configuration conf) {
    return isServedScheme(scheme, () -> conf);
  }

  /** {@link #isServedScheme(String, Configuration)} over the job's storage configuration. */
  public static boolean isServedScheme(String scheme, StorageConfiguration<?> storageConf) {
    return isServedScheme(scheme, () -> storageConf.unwrapAs(Configuration.class));
  }

  /**
   * Whether {@code uri} names a scheme that requires an authority ({@link #AUTHORITY_REQUIRED_SCHEMES}) without one, and Hadoop
   * cannot supply it from the same-scheme default file system.
   * An invalid or unavailable job configuration is not proof that the reference is malformed, so it answers false.
   */
  static boolean isMissingRequiredAuthority(URI uri, Configuration conf) {
    return isMissingRequiredAuthority(uri, () -> conf);
  }

  /** {@link #isMissingRequiredAuthority(URI, Configuration)} over the job's storage configuration. */
  static boolean isMissingRequiredAuthority(URI uri, StorageConfiguration<?> storageConf) {
    return isMissingRequiredAuthority(uri, () -> storageConf.unwrapAs(Configuration.class));
  }

  /**
   * Whether {@code cause}, met while the storage for {@code uri} is constructed, reports the reference's invalid authority: an
   * authority in which the URI parser found no host, rejected with an argument failure or a null host.
   */
  public static boolean isInvalidAuthorityFailure(URI uri, Throwable cause) {
    return uri.getAuthority() != null && uri.getHost() == null
        && (cause instanceof IllegalArgumentException || cause instanceof NullPointerException);
  }

  /**
   * Whether a failure to construct the storage for {@code uri}, once the caller's own precedence has classified nothing, is the
   * reference's own: a required authority missing ({@link #isMissingRequiredAuthority}), or a port above 65535 that construction
   * rejected with an argument failure (HDFS). A host that does not resolve is not: it cannot be told from a DNS outage.
   */
  public static boolean isMalformedConstruction(URI uri, Throwable failure, Configuration conf) {
    return isMalformedConstruction(uri, failure, () -> conf);
  }

  /** {@link #isMalformedConstruction(URI, Throwable, Configuration)} over the job's storage configuration. */
  public static boolean isMalformedConstruction(URI uri, Throwable failure, StorageConfiguration<?> storageConf) {
    return isMalformedConstruction(uri, failure, () -> storageConf.unwrapAs(Configuration.class));
  }

  private static boolean isMalformedConstruction(URI uri, Throwable failure, Supplier<Configuration> conf) {
    return isMissingRequiredAuthority(uri, conf) || (uri.getPort() > MAX_PORT && hasArgumentFailure(failure));
  }

  private static boolean hasArgumentFailure(Throwable failure) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      if (cause instanceof IllegalArgumentException) {
        return true;
      }
    }
    return false;
  }

  private static boolean isServedScheme(String scheme, Supplier<Configuration> conf) {
    if (scheme == null) {
      return false;
    }
    String lower = scheme.toLowerCase(Locale.ROOT);
    if (StorageSchemes.isSchemeSupported(lower)) {
      return true;
    }
    try {
      return conf.get().get("fs." + lower + ".impl") != null;
    } catch (RuntimeException configurationFailure) {
      // an unreadable job configuration is a platform failure, not proof that the scheme is foreign
      return true;
    }
  }

  private static boolean isMissingRequiredAuthority(URI uri, Supplier<Configuration> conf) {
    if (uri.getAuthority() != null || uri.getScheme() == null
        || !AUTHORITY_REQUIRED_SCHEMES.contains(uri.getScheme().toLowerCase(Locale.ROOT))) {
      return false;
    }
    try {
      URI defaultUri = FileSystem.getDefaultUri(conf.get());
      return !uri.getScheme().equalsIgnoreCase(defaultUri.getScheme()) || defaultUri.getAuthority() == null;
    } catch (RuntimeException configurationFailure) {
      // an invalid or unavailable job configuration is a platform failure, not proof that this reference is malformed
      return false;
    }
  }

  /** Enforces layout-one ownership when writing a completed blob reference. */
  public static void validateWriteOwnership(String externalPath, boolean managed) {
    if (externalPath == null || externalPath.isEmpty()) {
      throw new IllegalArgumentException("A written blob reference must have a non-empty external_path");
    }
    boolean hasScheme = new StoragePath(externalPath).toUri().getScheme() != null;
    if (hasScheme && managed) {
      throw new IllegalArgumentException("Blob external_path '" + externalPath + "' has a URI scheme and requires managed=false");
    }
    if (!hasScheme && !managed) {
      throw new IllegalArgumentException("Blob external_path '" + externalPath + "' is table-relative and requires managed=true");
    }
  }

}
