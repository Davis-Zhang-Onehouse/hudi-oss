/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.transform.Transformer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Composition rules of an unstructured job's required transformer and final schema guard. */
final class UnstructuredTransformerChain {

  /** The delimiter of a chain entry's {@code <id>:<class>} form ({@link org.apache.hudi.utilities.transform.ChainedTransformer}). */
  private static final String ID_DELIMITER = ":";

  private UnstructuredTransformerChain() {
  }

  static List<String> compose(UnstructuredMode mode, List<String> configured) {
    List<String> chain = composeRequired(mode, configured);
    Class<? extends Transformer> closing = UnstructuredSchemaGuard.class;
    String sourceName = "UnstructuredDataSource";
    boolean withIds = false;
    Set<String> ids = new HashSet<>();
    for (String entry : chain) {
      String trimmed = entry.trim();
      int delimiter = trimmed.indexOf(ID_DELIMITER);
      if (delimiter >= 0) {
        withIds = true;
        ids.add(trimmed.substring(0, delimiter).trim());
      }
      if (isA(trimmed.substring(delimiter + 1).trim(), closing)) {
        throw new HoodieTransformPlanException(sourceName + " runs " + closing.getName() + " last itself: remove it from "
            + "--transformer-class. Transformers configured: " + configured);
      }
    }
    String id = closing.getSimpleName();
    if (withIds && ids.contains(id)) {
      throw new HoodieTransformPlanException("--transformer-class uses the id " + id + ", which " + sourceName + " gives the "
          + closing.getName() + " it adds last: rename that entry. Transformers configured: " + configured);
    }
    List<String> closed = new ArrayList<>(chain);
    closed.add(withIds ? id + ID_DELIMITER + closing.getName() : closing.getName());
    return closed;
  }

  /**
   * Whether the chain entry {@code entry} ({@code <class>} or {@code <id>:<class>}) names {@code transformer} or a subclass of it.
   */
  public static boolean names(String entry, Class<?> transformer) {
    String trimmed = entry.trim();
    return isA(trimmed.substring(trimmed.indexOf(ID_DELIMITER) + 1).trim(), transformer);
  }

  /** {@link #compose} before its closing transformer: the required transformer joins the job's entries. */
  private static List<String> composeRequired(UnstructuredMode mode, List<String> configured) {
    Class<? extends Transformer> required = mode.requiredTransformer();
    Set<Class<? extends Transformer>> excludedTransformers = mode.excludedTransformers();
    List<String> entries = configured == null ? Collections.emptyList() : configured;
    String sourceName = "UnstructuredDataSource";
    int requiredEntries = 0;
    boolean withIds = false;
    Set<String> ids = new HashSet<>();
    List<String> chain = new ArrayList<>(entries.size() + 1);
    for (String entry : entries) {
      String trimmed = entry.trim();
      int delimiter = trimmed.indexOf(ID_DELIMITER);
      if (delimiter >= 0) {
        withIds = true;
        ids.add(trimmed.substring(0, delimiter).trim());
      }
      String className = trimmed.substring(delimiter + 1).trim();
      for (Class<? extends Transformer> excluded : excludedTransformers) {
        if (isA(className, excluded)) {
          throw new HoodieTransformPlanException(sourceName + " does not run with " + excluded.getName() + ": --transformer-class names "
              + className + ". " + (sourceName + " runs " + required.getName() + ". ")
              + "Transformers configured: " + entries);
        }
      }
      if (isA(className, required)) {
        requiredEntries++;
        if (requiredEntries > 1) {
          continue;
        }
      }
      chain.add(entry);
    }
    if (requiredEntries > 0) {
      return requiredEntries == 1 ? configured : chain;
    }
    if (withIds) {
      String id = required.getSimpleName();
      if (ids.contains(id)) {
        throw new HoodieTransformPlanException("--transformer-class uses the id " + id + ", which " + sourceName + " gives the "
            + required.getName() + " it adds to the chain: rename that entry, or name " + required.getName() + " in the chain. "
            + "Transformers configured: " + entries);
      }
      chain.add(0, id + ID_DELIMITER + required.getName());
    } else {
      chain.add(0, required.getName());
    }
    return chain;
  }

  /**
   * Whether {@code className} is {@code transformer} or a subclass of it. The class is loaded without being initialised, so a
   * class whose static initialiser fails is still recognised; the chain itself instantiates it. A class not on the classpath is
   * not that transformer: the chain reports it when it loads the class.
   */
  private static boolean isA(String className, Class<?> transformer) {
    if (transformer.getName().equals(className)) {
      return true;
    }
    try {
      return transformer.isAssignableFrom(Class.forName(className, false, UnstructuredTransformerChain.class.getClassLoader()));
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
