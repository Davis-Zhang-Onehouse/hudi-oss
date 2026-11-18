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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestCompletedWrite {
  @ParameterizedTest
  @ValueSource(strings = {"REQUESTED", "INFLIGHT", "COMPLETED"})
  void legacyArchiveProofRequiresCompletion(String state) {
    GenericRecord record = new GenericData.Record(HoodieArchivedMetaEntry.getClassSchema());
    record.put("actionType", "commit");
    record.put("actionState", state);
    assertEquals("COMPLETED".equals(state), assertDoesNotThrow(() -> CompletedWrite.isCompletedArchiveRecord(record)));
  }

  @Test
  void legacyArchiveWithoutStateUsesItsDocumentedCompletedDefault() {
    GenericRecord record = new GenericData.Record(HoodieArchivedMetaEntry.getClassSchema());
    record.put("actionType", "commit");
    assertTrue(assertDoesNotThrow(() -> CompletedWrite.isCompletedArchiveRecord(record)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"commit", "deltacommit", "replacecommit"})
  void lsmCompletedDataWritesRemainRecognized(String action) {
    GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
    record.put("action", action);
    assertTrue(CompletedWrite.isCompletedArchiveRecord(record));
  }

  @Test
  void tableServiceRecordIsNotProofOfADataWrite() {
    GenericRecord record = new GenericData.Record(HoodieLSMTimelineInstant.getClassSchema());
    record.put("action", "clean");
    assertFalse(CompletedWrite.isCompletedArchiveRecord(record));
  }
}
