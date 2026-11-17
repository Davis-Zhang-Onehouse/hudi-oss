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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import java.lang.reflect.InvocationTargetException;

/** Resolves the {@link SourceContract} of a source class. */
public final class SourceContracts {

  /** The contract of a source that requires nothing: no settings, no transformer, no schema check. */
  public static final SourceContract NONE = jobProps -> RequiredSourceSettings.NONE;

  private SourceContracts() {
  }

  /**
   * The contract of the source class {@code sourceClassName} for a job of {@code jobProps}: the one its
   * {@link SourceContractClass} names, on the class or a superclass, built from {@code jobProps}. The class is loaded but not
   * initialized. No class name, no annotation, or a class that cannot be loaded give {@link #NONE}; building the source then
   * reports a class that cannot be loaded.
   *
   * @throws RuntimeException the contract constructor's own, such as its refusal of the job's properties, as thrown
   * @throws HoodieException   when the contract class has no public constructor taking {@code TypedProperties}
   */
  public static SourceContract of(String sourceClassName, TypedProperties jobProps) {
    if (StringUtils.isNullOrEmpty(sourceClassName)) {
      return NONE;
    }
    Class<?> sourceClass;
    try {
      sourceClass = Class.forName(sourceClassName.trim(), false, SourceContracts.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      return NONE;
    }
    SourceContractClass named = sourceClass.getAnnotation(SourceContractClass.class);
    if (named == null) {
      return NONE;
    }
    try {
      return named.value().getConstructor(TypedProperties.class).newInstance(jobProps);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      throw new HoodieException("Unable to build the contract " + named.value().getName() + " of " + sourceClassName, e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new HoodieException("Unable to build the contract " + named.value().getName() + " of " + sourceClassName
          + ": it needs a public constructor taking TypedProperties", e);
    }
  }
}
