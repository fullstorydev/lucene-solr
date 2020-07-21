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
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
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
  private Map<String, ShardTerms> termsCache = new ConcurrentHashMap<>();
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

  private ShardTerms getTermsData(String collection, String shard, boolean forceFetch) {
    ShardTerms data = liveTerms.apply(collection, shard);
    if (data != null) return data;
    String key = collection + "/" + shard;
    if (forceFetch) termsCache.remove(key);
    ShardTerms terms = null;//termsCache.get(key); nocommit this is totally eliminating cache for debug purposes
    if (terms != null) {
      if (TimeUnit.SECONDS.convert(System.nanoTime() - terms.createTime, TimeUnit.NANOSECONDS) > CACHE_TIMEOUT) {
        termsCache.remove(key);
        return readTerms(collection, shard);
      } else {
        return terms;
      }
    }
    return readTerms(collection, shard);

  }

  private ShardTerms readTerms(String collection, String shard) {
    String znode = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms/" + shard;
    try {
      Stat stat = new Stat();
      byte[] data = zkStateReader.getZkClient().getData(znode, null, stat, true);
      return new ShardTerms((Map<String, Long>) Utils.fromJSON(data), stat.getVersion());
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
    ShardTerms terms = getTermsData(replica.collection, replica.slice, false);
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
    ShardTerms termsData = getTermsData(slice.collection, slice.getName(), false);
    return slice.getReplica(termsData.getLeader());
  }

  @Override
  public Replica getLeader(Slice slice, int timeout) throws InterruptedException {
    long startTime = System.nanoTime();
    long timeoutAt = startTime + NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    for (; ; ) {
      ShardTerms t = getTermsData(slice.collection, slice.getName(), false);
      String leader = t.getLeader();
      if (leader != null) {
        return slice.getReplica(leader);
      }

      if (System.nanoTime() > timeoutAt) {
        log.info("Could not find leader for {}/{} ", slice.getCollection(), slice.getName());
        return null;
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


}
