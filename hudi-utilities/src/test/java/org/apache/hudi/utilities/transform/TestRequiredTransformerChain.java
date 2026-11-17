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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.sources.RequiredSourceSettings;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.SourceContracts;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The chain the streamer runs for a source whose contract requires a transformer, around the job's own transformers. */
public class TestRequiredTransformerChain {

  private static final String REQUIRED = Required.class.getName();
  private static final String OTHER = Other.class.getName();
  private static final String LAST = Last.class.getName();

  /** The contract of a source that requires {@link Required} and excludes {@link Excluded}; refusals name it RequiringSource. */
  static class RequiringContract implements SourceContract {
    @Override
    public RequiredSourceSettings requiredSettings(TypedProperties jobProps) {
      return RequiredSourceSettings.NONE;
    }

    @Override
    public String sourceName() {
      return "RequiringSource";
    }

    @Override
    public Option<Class<? extends Transformer>> requiredTransformer() {
      return Option.of(Required.class);
    }

    @Override
    public Set<Class<? extends Transformer>> excludedTransformers() {
      return Collections.singleton(Excluded.class);
    }
  }

  @Test
  void sourceWithoutARequiredTransformerKeepsTheJobsChain() {
    List<String> configured = Arrays.asList(OTHER, LAST);
    assertSame(configured, RequiredTransformerChain.compose(SourceContracts.NONE, configured));
    assertNull(RequiredTransformerChain.compose(SourceContracts.NONE, null), "no chain stays no chain");
  }

  @Test
  void requiredTransformerRunsWhenTheJobConfiguresNone() {
    assertEquals(Collections.singletonList(REQUIRED), RequiredTransformerChain.compose(new RequiringContract(), null));
    assertEquals(Collections.singletonList(REQUIRED), RequiredTransformerChain.compose(new RequiringContract(), Collections.emptyList()));
  }

  /** The job's own transformers run after the required one, on the rows it produced, in the job's order. */
  @Test
  void requiredTransformerRunsFirstWhenTheJobDoesNotNameIt() {
    assertEquals(Arrays.asList(REQUIRED, OTHER, LAST), RequiredTransformerChain.compose(new RequiringContract(), Arrays.asList(OTHER, LAST)));
    // a class not on the classpath is not the required transformer: the chain reports it when it loads the class
    assertEquals(Arrays.asList(REQUIRED, "com.example.Missing"),
        RequiredTransformerChain.compose(new RequiringContract(), Collections.singletonList("com.example.Missing")));
  }

  /** A job that names the required transformer (or a subclass) keeps its position, so its transformers can run before it. */
  @Test
  void jobThatNamesTheRequiredTransformerKeepsItsChain() {
    for (List<String> configured : Arrays.asList(Arrays.asList(OTHER, REQUIRED, LAST), Arrays.asList(OTHER, " " + REQUIRED + " "),
        Arrays.asList("first:" + OTHER, "req:" + REQUIRED), Collections.singletonList(RequiredSubclass.class.getName()))) {
      assertSame(configured, RequiredTransformerChain.compose(new RequiringContract(), configured), configured.toString());
    }
  }

  /** In a chain of {@code <id>:<class>} entries, the added entry takes the required transformer's simple name as its id. */
  @Test
  void requiredTransformerTakesAnIdInAChainWithIds() {
    List<String> chain = RequiredTransformerChain.compose(new RequiringContract(), Arrays.asList("a:" + OTHER, "b:" + LAST));
    assertEquals(Arrays.asList("Required:" + REQUIRED, "a:" + OTHER, "b:" + LAST), chain);
    // the chain loads: every entry has an id
    ChainedTransformer chained = new ChainedTransformer(chain, Option::empty);
    assertEquals(Arrays.asList(REQUIRED, OTHER, LAST), chained.getTransformersNames());

    HoodieTransformPlanException taken = assertThrows(HoodieTransformPlanException.class,
        () -> RequiredTransformerChain.compose(new RequiringContract(), Arrays.asList("Required:" + OTHER)));
    assertTrue(taken.getMessage().contains("uses the id Required"), taken.getMessage());
  }

  @Test
  void requiredTransformerNamedTwiceRunsOnceAtItsFirstPosition() {
    assertRequiredRunsOnce(Arrays.asList(OTHER, REQUIRED, LAST, REQUIRED), Arrays.asList(OTHER, REQUIRED, LAST));
    assertRequiredRunsOnce(Arrays.asList(REQUIRED, OTHER, RequiredSubclass.class.getName(), REQUIRED, LAST),
        Arrays.asList(REQUIRED, OTHER, LAST));
    assertRequiredRunsOnce(Arrays.asList(OTHER, RequiredSubclass.class.getName(), REQUIRED, LAST),
        Arrays.asList(OTHER, RequiredSubclass.class.getName(), LAST));
    assertRequiredRunsOnce(Arrays.asList(OTHER, REQUIRED, " " + REQUIRED + " ", LAST), Arrays.asList(OTHER, REQUIRED, LAST));
    assertRequiredRunsOnce(Arrays.asList(OTHER, REQUIRED, OTHER, REQUIRED, LAST), Arrays.asList(OTHER, REQUIRED, OTHER, LAST));
    assertRequiredRunsOnce(Arrays.asList("first:" + OTHER, "req:" + REQUIRED, "last:" + LAST, "duplicate:" + REQUIRED),
        Arrays.asList("first:" + OTHER, "req:" + REQUIRED, "last:" + LAST));
    assertRequiredRunsOnce(Arrays.asList("first:" + OTHER, "req:" + REQUIRED, "req:" + REQUIRED, "last:" + LAST),
        Arrays.asList("first:" + OTHER, "req:" + REQUIRED, "last:" + LAST));
  }

  private void assertRequiredRunsOnce(List<String> configured, List<String> expected) {
    List<String> original = new ArrayList<>(configured);
    List<String> chain = RequiredTransformerChain.compose(new RequiringContract(), configured);
    assertEquals(expected, chain);
    assertEquals(original, configured, "the job's configured chain must not be mutated");
    Required.INVOCATIONS.set(0);
    new ChainedTransformer(chain, Option::empty).apply(null, null, null, new TypedProperties());
    assertEquals(1, Required.INVOCATIONS.get(), "the required transformer must run once");
  }

  @Test
  void excludedTransformerIsRefused() {
    for (String excluded : new String[] {Excluded.class.getName(), "x:" + ExcludedSubclass.class.getName()}) {
      HoodieTransformPlanException refused = assertThrows(HoodieTransformPlanException.class,
          () -> RequiredTransformerChain.compose(new RequiringContract(), Arrays.asList(REQUIRED, REQUIRED, OTHER, excluded)));
      assertTrue(refused.getMessage().startsWith("RequiringSource does not run with " + Excluded.class.getName()), refused.getMessage());
    }
  }

  /** The probe loads a configured class without initialising it: one whose static initialiser fails is still recognised. */
  @Test
  void probeDoesNotInitialiseTheConfiguredClass() {
    List<String> configured = Collections.singletonList(FailingInitialiser.class.getName());
    assertSame(configured, RequiredTransformerChain.compose(new RequiringContract(), configured));
    assertFalse(FailingInitialiser.Probe.INITIALISED.get(), "the probe initialised the configured transformer class");
  }

  /** A transformer returning its input. */
  abstract static class Identity implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      return rowDataset;
    }
  }

  public static class Required extends Identity {
    static final AtomicInteger INVOCATIONS = new AtomicInteger();

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      INVOCATIONS.incrementAndGet();
      return rowDataset;
    }
  }

  public static class RequiredSubclass extends Required {
  }

  public static class Excluded extends Identity {
  }

  public static class ExcludedSubclass extends Excluded {
  }

  public static class Other extends Identity {
  }

  public static class Last extends Identity {
  }

  /** A required transformer subclass whose static initialiser records itself and fails; the probe must not run it. */
  public static class FailingInitialiser extends Required {
    /** Holds the flag outside the failing class, which is unreadable once its initialisation has failed. */
    static final class Probe {
      static final AtomicBoolean INITIALISED = new AtomicBoolean();
    }

    static {
      // records the initialisation, then fails it (behind a condition: a static initialiser must be able to complete normally)
      if (Probe.INITIALISED.compareAndSet(false, true)) {
        throw new IllegalStateException("injected: a transformer whose static initialiser fails");
      }
    }
  }

  /** A source that requires {@link Required} and closes every chain with {@link Last}. */
  static class ClosingContract extends RequiringContract {
    @Override
    public Option<Class<? extends Transformer>> closingTransformer() {
      return Option.of(Last.class);
    }
  }

  @Test
  void closingTransformerIsAppendedLastInBothForms() {
    assertEquals(Arrays.asList(REQUIRED, LAST), RequiredTransformerChain.compose(new ClosingContract(), null));
    assertEquals(Arrays.asList(REQUIRED, OTHER, LAST), RequiredTransformerChain.compose(new ClosingContract(), Collections.singletonList(OTHER)));
    assertEquals(Arrays.asList(OTHER, REQUIRED, LAST), RequiredTransformerChain.compose(new ClosingContract(), Arrays.asList(OTHER, REQUIRED)));
    assertEquals(Arrays.asList("Required:" + REQUIRED, "o:" + OTHER, "Last:" + LAST),
        RequiredTransformerChain.compose(new ClosingContract(), Collections.singletonList("o:" + OTHER)));
  }

  @Test
  void jobNamingTheClosingTransformerIsRefused() {
    HoodieTransformPlanException named = assertThrows(HoodieTransformPlanException.class,
        () -> RequiredTransformerChain.compose(new ClosingContract(), Arrays.asList(OTHER, LAST)));
    assertTrue(named.getMessage().contains("runs " + LAST + " last itself"), named.getMessage());
    HoodieTransformPlanException id = assertThrows(HoodieTransformPlanException.class,
        () -> RequiredTransformerChain.compose(new ClosingContract(), Collections.singletonList("Last:" + OTHER)));
    assertTrue(id.getMessage().contains("uses the id Last"), id.getMessage());
  }

  @Test
  void entryNamesItsTransformerInBothForms() {
    assertTrue(RequiredTransformerChain.names(REQUIRED, Required.class));
    assertTrue(RequiredTransformerChain.names(" r : " + RequiredSubclass.class.getName(), Required.class));
    assertFalse(RequiredTransformerChain.names(OTHER, Required.class));
  }
}
