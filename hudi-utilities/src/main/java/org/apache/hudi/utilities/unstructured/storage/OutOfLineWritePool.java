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

import org.apache.hudi.exception.HoodieException;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs one task's out-of-line writes ({@link OutOfLineWriter#writeAt}: a marker, then the file) on {@link #THREADS} background
 * threads, so a task prepares its next payloads while the timeline server batches the markers (every
 * {@code hoodie.markers.timeline_server_based.batch.interval_ms}, 50 ms by default) and storage takes the PUTs. The bytes of
 * the payloads submitted and not yet written are bounded by {@link #MAX_PENDING_BYTES} (one larger payload is admitted alone):
 * a caller blocks until enough earlier writes finish. The caller holds one more payload while it waits, so a task holds at most
 * about twice that.
 *
 * <p>The first failure of a write is kept and thrown, as it was raised (later ones suppressed into it), by the next
 * {@link #submit} or by {@link #awaitAll}, which the task calls before it reports its end: a write that failed fails the task, as
 * a write on the task's own thread does. {@link #close} drops the writes not yet started and waits, through interrupts, for
 * those running to end, so no file of the task is created after it ends; a rollback of the round deletes every marked file.
 */
public final class OutOfLineWritePool implements Closeable {

  /** Writes in flight at once per task. */
  static final int THREADS = 4;
  /** 4 MiB: the bytes of the payloads waiting to be written, per task. */
  public static final int MAX_PENDING_BYTES = 4 * 1024 * 1024;

  private static final AtomicInteger POOLS = new AtomicInteger();

  private final Semaphore pendingBytes = new Semaphore(MAX_PENDING_BYTES);
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final ExecutorService threads;
  private int submitted;
  private final AtomicInteger finished = new AtomicInteger();
  private final Object done = new Object();

  public OutOfLineWritePool() {
    int pool = POOLS.incrementAndGet();
    AtomicInteger thread = new AtomicInteger();
    this.threads = new ThreadPoolExecutor(THREADS, THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), runnable -> {
      Thread t = new Thread(runnable, "hudi-out-of-line-write-" + pool + "-" + thread.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
  }

  /** One write: it throws what the task should fail with. */
  @FunctionalInterface
  public interface Write {
    void run() throws Exception;
  }

  /**
   * Runs {@code write}, which holds {@code bytes} bytes until it ends, in the background; first throws the failure of an
   * earlier write, if any.
   *
   * @throws HoodieException when interrupted while waiting for room (the interrupt is kept), or when the pool is closed
   */
  public void submit(long bytes, Write write) {
    rethrowFailure();
    if (threads.isShutdown()) {
      throw new HoodieException("The out-of-line write pool is closed");
    }
    int permits = (int) Math.min(MAX_PENDING_BYTES, Math.max(1L, bytes));
    try {
      pendingBytes.acquire(permits);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while waiting to write an out-of-line payload", e);
    }
    submitted++;
    try {
      threads.execute(new Pending(permits, write));
    } catch (RejectedExecutionException e) {
      submitted--;
      pendingBytes.release(permits);
      throw new HoodieException("The out-of-line write pool is closed", e);
    }
  }

  /** A submitted write and the permits it holds until it ends or is dropped. */
  private final class Pending implements Runnable {
    private final int permits;
    private final Write write;

    Pending(int permits, Write write) {
      this.permits = permits;
      this.write = write;
    }

    @Override
    public void run() {
      try {
        write.run();
      } catch (Throwable t) {
        if (!failure.compareAndSet(null, t) && failure.get() != t) {
          failure.get().addSuppressed(t);
        }
      } finally {
        end();
      }
    }

    /** Gives back the permits and counts the write as ended: it ran, or close dropped it. */
    void end() {
      pendingBytes.release(permits);
      finished.incrementAndGet();
      synchronized (done) {
        done.notifyAll();
      }
    }
  }

  /** Throws the first failure of a write so far, if any. */
  public void checkFailure() {
    rethrowFailure();
  }

  /** Waits for every write submitted so far, then throws the first failure, if any. */
  public void awaitAll() {
    synchronized (done) {
      while (finished.get() < submitted) {
        try {
          done.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HoodieException("Interrupted while waiting for out-of-line writes", e);
        }
      }
    }
    rethrowFailure();
  }

  /**
   * Stops taking writes, drops those not yet started (after {@link #awaitAll} there are none; otherwise the task is failing;
   * a drop is recorded as a failure, so a later {@link #awaitAll} or {@link #checkFailure} throws) and asks those running to
   * stop, then waits for them to end, through interrupts: a store or a marker call may ignore an
   * interrupt, and its file must not be created after the task ends. The interrupt is kept. Failures are not thrown here.
   */
  @Override
  public void close() {
    List<Runnable> dropped = threads.shutdownNow();
    if (!dropped.isEmpty()) {
      // their files were never written: a barrier after this close fails rather than pass without them
      HoodieException notWritten = new HoodieException(dropped.size() + " out-of-line writes were dropped by close before they ran");
      if (!failure.compareAndSet(null, notWritten)) {
        failure.get().addSuppressed(notWritten);
      }
    }
    for (Runnable each : dropped) {
      // never run: its permits go back and it counts as ended, so no awaitAll or submit waits for it
      ((Pending) each).end();
    }
    boolean interrupted = false;
    boolean terminated = false;
    while (!terminated) {
      try {
        terminated = threads.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private void rethrowFailure() {
    Throwable first = failure.get();
    if (first == null) {
      return;
    }
    if (first instanceof RuntimeException) {
      throw (RuntimeException) first;
    }
    if (first instanceof Error) {
      throw (Error) first;
    }
    throw new HoodieException("An out-of-line write failed", first);
  }
}
