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

import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The out-of-line write pool's thread rules: a file is named with the write token of the thread that asks for its path (the
 * task's), never the pool thread's; close waits, through interrupts, for a running write that ignores them; and a closed pool
 * refuses writes.
 */
class TestOutOfLineWritePool {

  private static final String INSTANT = "20261110120000000";

  @TempDir
  Path temp;

  /** A task context whose token is 7-8-9 on the thread that built it (the task's) and 99-99-99 on any other. */
  private static final class ThreadTaskContext extends TaskContextSupplier {
    private final transient Thread task = Thread.currentThread();

    private <T> Supplier<T> onTask(T taskValue, T otherValue) {
      return () -> Thread.currentThread() == task ? taskValue : otherValue;
    }

    @Override
    public Supplier<Integer> getPartitionIdSupplier() {
      return onTask(7, 99);
    }

    @Override
    public Supplier<Integer> getStageIdSupplier() {
      return onTask(8, 99);
    }

    @Override
    public Supplier<Long> getAttemptIdSupplier() {
      return onTask(9L, 99L);
    }

    @Override
    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }

    @Override
    public Supplier<Integer> getTaskAttemptNumberSupplier() {
      return onTask(0, 0);
    }

    @Override
    public Supplier<Integer> getStageAttemptNumberSupplier() {
      return onTask(0, 0);
    }
  }

  @Test
  void fileWrittenOnThePoolCarriesTheTokenOfTheTaskThatNamedIt() throws Exception {
    String base = temp.toUri().toString();
    HoodieStorage storage = new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration()));
    OutOfLineWriter writer = new OutOfLineWriter(storage, base, INSTANT, Option.empty(), new ThreadTaskContext());
    byte[] bytes = {4, 5, 6};
    String relative = writer.newPath("p");
    try (OutOfLineWritePool pool = new OutOfLineWritePool()) {
      pool.submit(bytes.length, () -> writer.writeAt(relative, out -> out.write(bytes)));
      pool.awaitAll();
    }
    assertEquals("7-8-9", FSUtils.getWriteTokenFromBaseFile(relative.substring(relative.lastIndexOf('/') + 1)));
    assertArrayEquals(bytes, Files.readAllBytes(temp.resolve(relative)));
  }

  @Test
  void closeOnAnInterruptedThreadWaitsForARunningWriteThatIgnoresInterrupts() throws Exception {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
    pool.submit(1, () -> {
      started.countDown();
      long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
      while (System.nanoTime() < until) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignored) {
          // as a store or marker call that swallows the interrupt and goes on
        }
      }
      finished.set(true);
    });
    assertTrue(started.await(10, TimeUnit.SECONDS));
    Thread.currentThread().interrupt();
    try {
      pool.close();
      assertTrue(finished.get(), "close returned before the running write ended");
      assertTrue(Thread.currentThread().isInterrupted(), "the interrupt is kept");
    } finally {
      Thread.interrupted();
    }
  }

  /**
   * Writes close drops before they start give back their room and count as ended, so nothing waits for them, and the
   * barrier fails: their files were never written.
   */
  @Test
  void writesDroppedByCloseReleaseTheirRoomAndEnd() throws Exception {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    CountDownLatch hold = new CountDownLatch(1);
    CountDownLatch running = new CountDownLatch(4);
    for (int i = 0; i < 4; i++) {
      pool.submit(1, () -> {
        running.countDown();
        boolean released = false;
        while (!released) {
          try {
            released = hold.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException ignored) {
            // a running write that ignores the interrupt close() sends
          }
        }
      });
    }
    assertTrue(running.await(10, TimeUnit.SECONDS), "the 4 threads are busy");
    for (int i = 0; i < 3; i++) {
      // queued behind the busy threads: dropped by close
      pool.submit(OutOfLineWritePool.MAX_PENDING_BYTES / 4, () -> {
        throw new IllegalStateException("a dropped write never runs");
      });
    }
    Thread closer = new Thread(pool::close);
    closer.start();
    // close has dropped the queue once it waits for the running writes
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (closer.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertEquals(Thread.State.TIMED_WAITING, closer.getState(), "close waits for the running writes");
    hold.countDown();
    closer.join(TimeUnit.SECONDS.toMillis(30));
    assertFalse(closer.isAlive(), "close returned");
    // the dropped writes ended (awaitAll does not wait for them) and none ran (their own failure is not the one thrown)
    HoodieException notWritten = assertThrows(HoodieException.class, pool::awaitAll);
    assertTrue(notWritten.getMessage().contains("3 out-of-line writes were dropped"), notWritten.getMessage());
  }

  @Test
  void closedPoolRefusesWrites() {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    pool.close();
    HoodieException refused = assertThrows(HoodieException.class, () -> pool.submit(1, () -> { }));
    assertTrue(refused.getMessage().contains("closed"), refused.getMessage());
  }

  @Test
  void laterFailuresAreSuppressedIntoTheFirst() {
    OutOfLineWritePool pool = new OutOfLineWritePool();
    CountDownLatch go = new CountDownLatch(1);
    for (int i = 0; i < 3; i++) {
      int n = i;
      pool.submit(1, () -> {
        go.await();
        throw new IllegalStateException("write " + n);
      });
    }
    go.countDown();
    IllegalStateException first = assertThrows(IllegalStateException.class, pool::awaitAll);
    assertEquals(2, first.getSuppressed().length, "the two later failures");
    pool.close();
  }
}
