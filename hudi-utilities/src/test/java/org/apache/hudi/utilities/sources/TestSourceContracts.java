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
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSourceContracts {

  public abstract static class RegisteredSubclass extends UnstructuredDataSource {
    private RegisteredSubclass() {
      super(null, null, null, (org.apache.hudi.utilities.schema.SchemaProvider) null, null);
    }

    static final int VALUE = fail();

    private static int fail() {
      throw new IllegalStateException("initialized");
    }
  }

  @Test
  void ordinaryOrUnknownSourcesKeepTheirPropertiesAndChain() {
    TypedProperties props = new TypedProperties();
    List<String> chain = Collections.singletonList("user.Transformer");
    for (String source : new String[] {null, "", "missing.Source", String.class.getName()}) {
      SourceContract contract = SourceContracts.of(source, props);
      assertSame(SourceContracts.NONE, contract);
      assertSame(props, contract.prepareJob(props));
      assertSame(chain, contract.transformerChain(chain));
      assertNull(contract.transformerChain(null));
      assertTrue(contract.tableProperties().isEmpty());
    }
  }

  @Test
  void registeredSourceAndSubclassResolveWithoutInitialization() {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "blob");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    for (Class<?> source : new Class<?>[] {UnstructuredDataSource.class, RegisteredSubclass.class}) {
      SourceContract contract = SourceContracts.of(" " + source.getName() + " ", props);
      assertTrue(contract instanceof UnstructuredSourceContract);
      assertEquals("blob", ((UnstructuredSourceContract) contract).mode().configValue());
    }
  }

  @Test
  void registrationPropagatesInvalidJobAsThrown() {
    assertThrows(IllegalArgumentException.class,
        () -> SourceContracts.of(UnstructuredDataSource.class.getName(), new TypedProperties()));
  }
}
