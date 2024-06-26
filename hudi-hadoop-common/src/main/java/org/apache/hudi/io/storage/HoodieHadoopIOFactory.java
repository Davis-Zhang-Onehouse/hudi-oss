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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.hadoop.HoodieAvroFileReaderFactory;
import org.apache.hudi.io.hadoop.HoodieAvroFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

/**
 * Creates readers and writers for AVRO record payloads.
 * Currently uses reflection to support SPARK record payloads but
 * this ability should be removed with [HUDI-7746]
 */
public class HoodieHadoopIOFactory extends HoodieIOFactory {

  public HoodieHadoopIOFactory(StorageConfiguration<?> storageConf) {
    super(storageConf);
  }

  @Override
  public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        return new HoodieAvroFileReaderFactory(storageConf);
      case SPARK:
        //TODO: remove this case [HUDI-7746]
        try {
          return (HoodieFileReaderFactory) ReflectionUtils
              .loadClass("org.apache.hudi.io.storage.HoodieSparkFileReaderFactory",
                  new Class<?>[] {StorageConfiguration.class}, storageConf);
        } catch (Exception e) {
          throw new HoodieException("Unable to create HoodieSparkFileReaderFactory", e);
        }
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported");
    }
  }

  @Override
  public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        return new HoodieAvroFileWriterFactory(storageConf);
      case SPARK:
        //TODO: remove this case [HUDI-7746]
        try {
          return (HoodieFileWriterFactory) ReflectionUtils
              .loadClass("org.apache.hudi.io.storage.HoodieSparkFileWriterFactory",
                  new Class<?>[] {StorageConfiguration.class}, storageConf);
        } catch (Exception e) {
          throw new HoodieException("Unable to create HoodieSparkFileWriterFactory", e);
        }
      default:
        throw new UnsupportedOperationException(recordType + " record type not supported");
    }
  }

  @Override
  public HoodieStorage getStorage(StoragePath storagePath) {
    return new HoodieHadoopStorage(storagePath, storageConf);
  }

  @Override
  public HoodieStorage getStorage(StoragePath path,
                                  boolean enableRetry,
                                  long maxRetryIntervalMs,
                                  int maxRetryNumbers,
                                  long initialRetryIntervalMs,
                                  String retryExceptions,
                                  ConsistencyGuard consistencyGuard) {
    return new HoodieHadoopStorage(path, storageConf, enableRetry, maxRetryIntervalMs,
        maxRetryNumbers, maxRetryIntervalMs, retryExceptions, consistencyGuard);
  }
}