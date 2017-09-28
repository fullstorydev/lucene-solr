package org.apache.solr.search.facet;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.FixedBitSet;

class FixedBitSetPool {

  private static final long MAX_POOL_SIZE;
  private static final int NUM_POOLS; // must be power of two for mask below to work
  private static final int POOLS_MASK;
  static {
    int numPools;
    try {
      numPools = Integer.valueOf(System.getProperty("solr.facet.bitset.poolnum"));
    } catch (NumberFormatException e) {
      numPools = 4;
    }
    if (numPools < 1) {
      numPools = 1;
    }
    // clamp to power of two
    if (Integer.bitCount(numPools) != 1) {
      int lead = Integer.numberOfLeadingZeros(numPools) - 1;
      if (lead <= 0) {
        lead = 1;
      }
      numPools = Integer.MIN_VALUE >>> lead;
    }
    NUM_POOLS = numPools;
    POOLS_MASK = numPools - 1;

    long maxPoolSize;
    try {
      maxPoolSize = Long.valueOf(System.getProperty("solr.facet.bitset.maxpoolsize"));
    } catch (NumberFormatException e) {
      maxPoolSize = Runtime.getRuntime().maxMemory() / 4 / numPools;
    }
    MAX_POOL_SIZE = maxPoolSize;
  }

  // multiple pools to reduce contention, threads are hashed to one of the pools
  private static final FixedBitSetPool[] pools = new FixedBitSetPool[NUM_POOLS];
  static {
    for (int i = 0; i < pools.length; i++) {
      pools[i] = new FixedBitSetPool();
    }
  }

  static FixedBitSetPool getPool() {
    return pools[Thread.currentThread().hashCode() & POOLS_MASK];
  }

  private final AtomicLong totalSize = new AtomicLong();
  private final AtomicLong poolHits = new AtomicLong();
  private final AtomicLong poolHitsSize = new AtomicLong();
  private final AtomicLong poolMisses = new AtomicLong();
  private final AtomicLong poolMissesSize = new AtomicLong();
  private final AtomicLong poolReturns = new AtomicLong();
  private final AtomicLong poolReturnsSize = new AtomicLong();
  private final AtomicLong poolReturnsRejected = new AtomicLong();
  private final AtomicLong poolReturnsRejectedSize = new AtomicLong();
  private final ConcurrentSkipListMap<Integer, ConcurrentStack<long[]>> entries = new ConcurrentSkipListMap<>();

  private static final class ConcurrentStack<T> {
    private static final class Node<T> extends AtomicReference<Node<T>> {
      private final T val;
      Node(T val) {
        this.val = val;
      }
    }

    private final AtomicReference<Node<T>> head = new AtomicReference<>();

    ConcurrentStack(T initialHead) {
      head.lazySet(new Node<T>(initialHead));
    }

    boolean push(T val) {
      Node<T> newHead = new Node<T>(val);
      while (true) {
        Node<T> h = head.get();
        if (h == null) {
          return false;
        }
        newHead.lazySet(h);
        if (head.compareAndSet(h, newHead)) {
          return true;
        }
      }
    }

    T pop() {
      while (true) {
        Node<T> h = head.get();
        if (h == null) {
          return null;
        }
        if (head.compareAndSet(h, h.get())) {
          return h.val;
        }
      }
    }

    boolean isEmpty() {
      return head.get() == null;
    }
  }

  private FixedBitSetPool() {
  }

  public FixedBitSet get(int numBits) {
    int size = FixedBitSet.bits2words(numBits);
    for (ConcurrentStack<long[]> stack : entries.tailMap(size).values()) {
      long[] arr = stack.pop();
      if (arr != null) {
        Arrays.fill(arr, 0);
        totalSize.addAndGet(-(arr.length<<3));
        if (stack.isEmpty()) {
          entries.remove(arr.length, stack);
        }
        poolHits.incrementAndGet();
        poolHitsSize.addAndGet(arr.length);
        return new FixedBitSet(arr, numBits);
      }
    }
    FixedBitSet ret = new FixedBitSet(numBits);
    poolMisses.incrementAndGet();
    poolMissesSize.addAndGet(ret.getBits().length);
    return ret;
  }

  public void put(FixedBitSet set) {
    long[] arr = set.getBits();

    while (true) {
      long sz = totalSize.get();
      long newSz = sz + (arr.length<<3);
      if (newSz > MAX_POOL_SIZE) {
        // pool is full
        poolReturnsRejected.incrementAndGet();
        poolReturnsRejectedSize.addAndGet(arr.length);
        return;
      }
      if (totalSize.compareAndSet(sz, newSz)) {
        break;
      }
    }

    Integer len = arr.length;
    while (true) {
      ConcurrentStack<long[]> stack = entries.get(len);
      if (stack == null) {
        ConcurrentStack<long[]> newStack = new ConcurrentStack<>(arr);
        stack = entries.putIfAbsent(len, newStack);
        if (stack == null) {
          poolReturns.incrementAndGet();
          poolReturnsSize.addAndGet(arr.length);
          return;
        }
      }
      if (stack.push(arr)) {
        poolReturns.incrementAndGet();
        poolReturnsSize.addAndGet(arr.length);
        return;
      }
    }
  }

  public static void main(String[] args) {
    System.out.println("MAX = " + MAX_POOL_SIZE + " (" + Runtime.getRuntime().maxMemory() + ")");
    FixedBitSetPool p = getPool();
    printStats(p);
    FixedBitSet fbs = p.get(100);
    printStats(p);
    p.put(fbs);
    printStats(p);
    fbs = p.get(100);
    printStats(p);
    p.put(fbs);
    printStats(p);
    fbs = p.get(1000000000);
    printStats(p);
    p.put(fbs);
    printStats(p);
    fbs = p.get(2000000000);
    printStats(p);
    p.put(fbs);
    printStats(p);
  }

  private static void printStats(FixedBitSetPool p) {
    System.out.println("CURRENT SIZE = " + p.totalSize.get());
    System.out.println("hits = " + p.poolHits.get() + " (" + p.poolHitsSize.get() + ")");
    System.out.println("misses = " + p.poolMisses.get() + " (" + p.poolMissesSize.get() + ")");
    System.out.println("returns = " + p.poolReturns.get() + " (" + p.poolReturnsSize.get() + ")");
    System.out.println("rejects = " + p.poolReturnsRejected.get() + " (" + p.poolReturnsRejectedSize.get() + ")");
    for (Map.Entry<Integer, ConcurrentStack<long[]>> entry : p.entries.entrySet()) {
      int c = countLen(entry.getValue());
      if (c > 0) {
        System.out.println("  " + entry.getKey() + ": " + c);
      }
    }
  }

  private static int countLen(ConcurrentStack<?> s) {
    int count = 0;
    ConcurrentStack.Node<?> n = s.head.get();
    while (n != null) {
      count++;
      n = n.get();
    }
    return count;
  }
}
