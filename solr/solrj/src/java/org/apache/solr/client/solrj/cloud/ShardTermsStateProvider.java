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

package org.apache.solr.client.solrj.cloud;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ShardTermsStateProvider implements ShardStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public final int CACHE_TIMEOUT = 60 * 5;//5 minutes
  private final ZkStateReader zkStateReader;
  /**
   * This data is cached and probably stale
   */
  private ShardTermsCache termsCache = new ShardTermsCache();
  /**
   * This node may be watching these shards all the time so, we don't need to cache it
   */
  private final BiFunction<String, String, ShardTerms> liveTerms;

  public ShardTermsStateProvider(ZkStateReader zkStateReader, BiFunction<String, String, ShardTerms> liveTerms) {
    this.liveTerms = liveTerms;
    this.zkStateReader = zkStateReader;
  }

  @Override
  public Replica getLeader(String coll, String slice) {
    DocCollection collection = zkStateReader.getCollection(coll);
    if (collection == null) return null;
    Slice sl = collection.getSlice(slice);
    if (sl == null) return null;
    return getLeader(sl);
  }

  /** Get the current terms data for a given collection/shard
   * @param collection Name of collection
   * @param shard Name of shard
   * @param minVersion if the cache has this (or newer) version locally, return from the cache.
   *                   If not,'0' fetch fresh data from ZK. If the value is -1 fetch anyway
   */
  public ShardTerms getTermsData(String collection, String shard, int minVersion) {
    ShardTerms data = liveTerms.apply(collection, shard);
    if (data != null) return data;
    return termsCache.get(collection, shard, minVersion);
  }

  private ShardTerms readTerms(String collection, String shard) {
    String znode = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms/" + shard;
    try {
      Stat stat = new Stat();
      byte[] data = zkStateReader.getZkClient().getData(znode, null, stat, true);
      return new ShardTerms((Map<String, Long>) Utils.fromJSON(data), stat.getVersion());
    } catch (KeeperException.NoNodeException nne) {
      return null;
    } catch (KeeperException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating shard term for collection: " + collection, e);
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating shard term for collection: " + collection, e);
    }

  }


  @Override
  public Replica.State getState(Replica replica) {
    if (!zkStateReader.isNodeLive(replica.getNodeName())) {
      return Replica.State.DOWN;
    }
    ShardTerms terms = getTermsData(replica.collection, replica.slice, 0);
    if (terms == null) {
      return Replica.State.DOWN;
    }

    if (terms.isRecovering(replica.getName())) {
      return Replica.State.RECOVERING;
    }

    if (terms.haveHighestTermValue(replica.getName())) {
      return Replica.State.ACTIVE;
    } else {
      return Replica.State.RECOVERING;
    }
  }

  @Override
  public Replica getLeader(Slice slice) {
    ShardTerms termsData = getTermsData(slice.collection, slice.getName(), 0);
    if(termsData == null) return null;
    return slice.getReplica(termsData.getLeader());
  }

  @Override
  public Replica getLeader(Slice slice, int timeout) throws InterruptedException {
    long startTime = System.nanoTime();
    if(timeout == -1) {
      timeout = ZkStateReader.GET_LEADER_RETRY_DEFAULT_TIMEOUT;
    }
    long timeoutAt = startTime + NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    for (; ; ) {
      ShardTerms t = getTermsData(slice.collection, slice.getName(), 0);
      if(t != null) {
        String leader = t.getLeader();
        if (leader != null) {
          return slice.getReplica(leader);
        }

        if (System.nanoTime() > timeoutAt) {
          log.info("Could not find leader for {}/{} ", slice.getCollection(), slice.getName());
          return null;
        }
      }
      log.trace("waiting for leader of {}/{}", slice.getCollection(), slice.getName());
      Thread.sleep(100);

    }
  }

  @Override
  public Replica getLeader(String collection, String slice, int timeout) throws InterruptedException {
    return getLeader(zkStateReader.getCollection(collection).getSlice(slice), timeout);
  }

  @Override
  public boolean isActive(Replica replica) {
    return getState(replica) == Replica.State.ACTIVE;
  }

  @Override
  public boolean isActive(Slice slice) {
    return false;
  }

  class ShardTermsCache {

    private final long TTL = 3 * 60 * 60 * 1000;//3 mins
    Map<String, Map<String, TermsHolder>> cache = new ConcurrentHashMap<>();


    public ShardTerms get(String coll, String slice, int expectedVersion) {
      Map<String, TermsHolder> slices = cache.get(coll);
      if (slices == null) {
        //we are not trying to be threadsafe here.
        // if multiple threads create this in parallel, there is one extra read from Zk and that's OK
        cache.put(coll, slices = new ConcurrentHashMap<>());
      }
      if (expectedVersion == -1) slices.remove(slice);
      TermsHolder terms = slices.get(slice);
      if (terms != null) {
        if (terms.terms.getVersion() >= expectedVersion && System.currentTimeMillis() < terms.expireAt) {
          return terms.terms;
        }
      }
      ShardTerms freshTerms = readTerms(coll, slice);
      if(freshTerms == null) return null;
      terms = new TermsHolder(freshTerms, System.currentTimeMillis() + TTL);
      slices.put(slice, terms);
      return terms.terms;
    }


  }
  static class TermsHolder {
    final ShardTerms terms;
    final long expireAt;

    TermsHolder(ShardTerms terms, long expireAt) {
      this.terms = terms;
      this.expireAt = expireAt;
    }
  }

}
