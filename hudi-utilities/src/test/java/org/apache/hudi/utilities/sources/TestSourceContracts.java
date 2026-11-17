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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** How the streamer finds a source's {@link SourceContract} from the source class's name, before the source is built. */
public class TestSourceContracts {

  /** A contract that records the properties it was built from. */
  public static class RecordingContract implements SourceContract {
    static final AtomicReference<TypedProperties> BUILT_WITH = new AtomicReference<>();

    public RecordingContract(TypedProperties props) {
      BUILT_WITH.set(props);
    }

    @Override
    public RequiredSourceSettings requiredSettings(TypedProperties jobProps) {
      return RequiredSourceSettings.builder("Recorded").require(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE, "7").build();
    }
  }

  /** A contract whose constructor refuses every job. */
  public static class RefusingContract implements SourceContract {
    public RefusingContract(TypedProperties props) {
      throw new IllegalArgumentException("RefusingSource refuses this job");
    }

    @Override
    public RequiredSourceSettings requiredSettings(TypedProperties jobProps) {
      return RequiredSourceSettings.NONE;
    }
  }

  /** A contract without the constructor taking the job's properties. */
  public static class NoPropsConstructorContract implements SourceContract {
    @Override
    public RequiredSourceSettings requiredSettings(TypedProperties jobProps) {
      return RequiredSourceSettings.NONE;
    }
  }

  /** A source class with no annotation; never instantiated. */
  public abstract static class PlainSource {
  }

  /** A source class naming its contract; never instantiated. */
  @SourceContractClass(RecordingContract.class)
  public abstract static class AnnotatedSource {
  }

  /** A subclass of an annotated source class, which inherits its contract. */
  public abstract static class SubclassOfAnnotatedSource extends AnnotatedSource {
  }

  @SourceContractClass(RefusingContract.class)
  public abstract static class RefusingSource {
  }

  @SourceContractClass(NoPropsConstructorContract.class)
  public abstract static class NoPropsConstructorSource {
  }

  /** A source class whose static initialiser fails: resolving its contract does not initialise it. */
  @SourceContractClass(RecordingContract.class)
  public abstract static class FailingInitialiserSource {
    static final int VALUE = fail();

    private static int fail() {
      throw new IllegalStateException("initialised");
    }
  }

  @Test
  void noClassNameOrNoAnnotationGivesNone() {
    TypedProperties props = new TypedProperties();
    assertSame(SourceContracts.NONE, SourceContracts.of(null, props));
    assertSame(SourceContracts.NONE, SourceContracts.of("", props));
    assertSame(SourceContracts.NONE, SourceContracts.of(PlainSource.class.getName(), props));
    assertSame(props, SourceContracts.NONE.requiredSettings(props).apply(props), "the no-op contract changes nothing");
  }

  @Test
  void annotationIsReadOnTheClassAndInheritedBySubclasses() {
    TypedProperties props = new TypedProperties();
    for (Class<?> source : new Class<?>[] {AnnotatedSource.class, SubclassOfAnnotatedSource.class, FailingInitialiserSource.class}) {
      RecordingContract.BUILT_WITH.set(null);
      SourceContract contract = SourceContracts.of(" " + source.getName() + " ", props);
      assertTrue(contract instanceof RecordingContract, source.getName());
      assertSame(props, RecordingContract.BUILT_WITH.get(), "the contract is built from the job's properties");
      assertEquals("7", contract.requiredSettings(props).apply(props).getString("hoodie.upsert.shuffle.parallelism"));
    }
  }

  /** A source class that cannot be loaded gives the no-op contract: building the source then reports it, as before. */
  @Test
  void unloadableClassGivesNoneWithoutThrowing() {
    assertSame(SourceContracts.NONE, SourceContracts.of("com.example.NoSuchSource", new TypedProperties()));
  }

  @Test
  void contractConstructorFailurePropagatesAsThrown() {
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> SourceContracts.of(RefusingSource.class.getName(), new TypedProperties()));
    assertEquals("RefusingSource refuses this job", refused.getMessage());
    HoodieException missing = assertThrows(HoodieException.class,
        () -> SourceContracts.of(NoPropsConstructorSource.class.getName(), new TypedProperties()));
    assertTrue(missing.getMessage().contains("needs a public constructor taking TypedProperties"), missing.getMessage());
  }
}
