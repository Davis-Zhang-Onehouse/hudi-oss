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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.sources.SourceContract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The transformer chain the streamer runs for its source: the job's {@code --transformer-class} list, plus the transformer a
 * source's contract requires ({@link SourceContract#requiredTransformer}). A job that does not name the required transformer gets it first in the chain, so
 * its own transformers run on the rows it produced. A job that names it once keeps that position; a source's contract may still
 * refuse entries before it (the unstructured data source does). When named more than once, only its first entry is kept. A
 * transformer the source excludes is refused. A source's closing transformer runs last, after the job's own; a job may not
 * name it.
 */
public final class RequiredTransformerChain {

  /** The delimiter of a chain entry's {@code <id>:<class>} form ({@link ChainedTransformer}). */
  private static final String ID_DELIMITER = ":";

  private RequiredTransformerChain() {
  }

  /**
   * The chain to run for the source of {@code contract}.
   *
   * @param contract   the contract of the streamer's source
   * @param configured the job's {@code --transformer-class} entries, or null when it configures none
   * @return {@code configured} itself for a source that requires no transformer, or when it names the required one exactly
   *     once and the source has no closing transformer; otherwise a new list keeping its first required entry, or adding it first
   *     when absent, then the source's closing transformer ({@link SourceContract#closingTransformer}) last. When the
   *     job's entries take the {@code <id>:<class>} form, an added entry takes its transformer's simple class name as its id.
   * @throws HoodieTransformPlanException when the chain names an excluded or the closing transformer, or already uses the id an
   *     added entry would take
   */
  public static List<String> compose(SourceContract contract, List<String> configured) {
    List<String> chain = composeRequired(contract, configured);
    Option<Class<? extends Transformer>> closing = contract.closingTransformer();
    if (!closing.isPresent()) {
      return chain;
    }
    String sourceName = contract.sourceName();
    boolean withIds = false;
    Set<String> ids = new HashSet<>();
    for (String entry : chain) {
      String trimmed = entry.trim();
      int delimiter = trimmed.indexOf(ID_DELIMITER);
      if (delimiter >= 0) {
        withIds = true;
        ids.add(trimmed.substring(0, delimiter).trim());
      }
      if (isA(trimmed.substring(delimiter + 1).trim(), closing.get())) {
        throw new HoodieTransformPlanException(sourceName + " runs " + closing.get().getName() + " last itself: remove it from "
            + "--transformer-class. Transformers configured: " + configured);
      }
    }
    String id = closing.get().getSimpleName();
    if (withIds && ids.contains(id)) {
      throw new HoodieTransformPlanException("--transformer-class uses the id " + id + ", which " + sourceName + " gives the "
          + closing.get().getName() + " it adds last: rename that entry. Transformers configured: " + configured);
    }
    List<String> closed = new ArrayList<>(chain);
    closed.add(withIds ? id + ID_DELIMITER + closing.get().getName() : closing.get().getName());
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
  private static List<String> composeRequired(SourceContract contract, List<String> configured) {
    Option<Class<? extends Transformer>> requiredOption = contract.requiredTransformer();
    Set<Class<? extends Transformer>> excludedTransformers = contract.excludedTransformers();
    if (!requiredOption.isPresent() && excludedTransformers.isEmpty()) {
      return configured;
    }
    List<String> entries = configured == null ? Collections.emptyList() : configured;
    String sourceName = contract.sourceName();
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
              + className + ". " + requiredOption.map(required -> sourceName + " runs " + required.getName() + ". ").orElse("")
              + "Transformers configured: " + entries);
        }
      }
      if (requiredOption.isPresent() && isA(className, requiredOption.get())) {
        requiredEntries++;
        if (requiredEntries > 1) {
          continue;
        }
      }
      chain.add(entry);
    }
    if (!requiredOption.isPresent()) {
      return configured;
    }
    if (requiredEntries > 0) {
      return requiredEntries == 1 ? configured : chain;
    }
    Class<? extends Transformer> required = requiredOption.get();
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
      return transformer.isAssignableFrom(Class.forName(className, false, RequiredTransformerChain.class.getClassLoader()));
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
