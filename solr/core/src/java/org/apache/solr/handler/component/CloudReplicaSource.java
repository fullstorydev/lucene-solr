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

package org.apache.solr.handler.component;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.OptimisticShardState;
import org.apache.solr.client.solrj.cloud.ShardStateProvider;
import org.apache.solr.client.solrj.cloud.ShardTermsStateProvider;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A replica source for solr cloud mode
 */
class CloudReplicaSource implements ShardStateAwareReplicaSource {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String[] slices;
  private List<String>[] replicas;
  private final String[] shardstr;

  private final Builder builder;
  ShardStateProvider shardStateProvider;

  private CloudReplicaSource(Builder builder) {
    this.builder = builder;
    if(builder.collection != null) {
      shardStateProvider = builder.zkStateReader.getShardStateProvider(builder.collection);
    }
    final String shards = builder.params.get(ShardParams.SHARDS);
    if (shards != null) {
      withShardsParam(builder, shards);
    } else {
      withClusterState(builder, builder.params);
    }
   shardstr = new String[slices.length];
  }

  public ShardStateProvider getShardStateProvider() {
    return shardStateProvider;
  }

  /**
   * Get a new instance of {@link CloudReplicaSource} with fresh details
   */
  public CloudReplicaSource refreshState(OptimisticShardState oss) {
    if(oss.getCollectionVer() >0) {
      //means the version we sent to the other server is stale
      DocCollection coll = builder.zkStateReader.getCollection(oss.getCollection());
      if(coll.getZNodeVersion() >=  oss.getCollectionVer()) {
        try {
          builder.zkStateReader.forceUpdateCollection(coll.getName());
        } catch (KeeperException | InterruptedException e) {
          //cannot update state.json
          log.error("Error refreshing Collection state ", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
    }
    int sliceVersion = oss.getSliceVersion(null, 0);
    if(sliceVersion >0) {
      ((ShardTermsStateProvider)shardStateProvider).getTermsData(oss.getCollection(),
          oss.getSliceName(),
          sliceVersion);
    }

    return new CloudReplicaSource(builder);
  }

  public OptimisticShardState getOptimisticShardState(int idx) {
    String sliceName = slices[idx];
    if(sliceName != null && shardStateProvider instanceof ShardTermsStateProvider) {
      return new OptimisticShardState.Builder()
          .withCollection(builder.collection, builder.zkStateReader.getCollection(builder.collection).getZNodeVersion())
          .addSlice(sliceName, ((ShardTermsStateProvider)shardStateProvider).getTermsData(builder.collection, sliceName, 0).getVersion())
          .build();
    }
    return null;

  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void withClusterState(Builder builder, SolrParams params) {
    ClusterState clusterState = builder.zkStateReader.getClusterState();
    String shardKeys = params.get(ShardParams._ROUTE_);

    // This will be the complete list of slices we need to query for this request.
    Map<String, Slice> sliceMap = new HashMap<>();

    // we need to find out what collections this request is for.

    // A comma-separated list of specified collections.
    // Eg: "collection1,collection2,collection3"
    String collections = params.get("collection");
    if (collections != null) {
      // If there were one or more collections specified in the query, split
      // each parameter and store as a separate member of a List.
      List<String> collectionList = StrUtils.splitSmart(collections, ",",
          true);
      // In turn, retrieve the slices that cover each collection from the
      // cloud state and add them to the Map 'slices'.
      for (String collectionName : collectionList) {
        // The original code produced <collection-name>_<shard-name> when the collections
        // parameter was specified (see ClientUtils.appendMap)
        // Is this necessary if ony one collection is specified?
        // i.e. should we change multiCollection to collectionList.size() > 1?
        addSlices(sliceMap, clusterState, params, collectionName, shardKeys, true);
      }
    } else {
      // just this collection
      addSlices(sliceMap, clusterState, params, builder.collection, shardKeys, false);
    }

    this.slices = sliceMap.keySet().toArray(new String[sliceMap.size()]);
    this.replicas = new List[slices.length];
    for (int i = 0; i < slices.length; i++) {
      String sliceName = slices[i];
      replicas[i] = findReplicas(builder, null, clusterState, sliceMap.get(sliceName));
    }
  }

  @Override
  public String getSliceShardsStr(int i) {
    return shardstr[i] = concat(getReplicasBySlice(i));
  }

  @Override
  public int getSliceId(String shard) {
    for (int i = 0; i < shardstr.length; i++) {
      if(Objects.equals(shardstr[i], shard)) return i;
    }
    return -1;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void withShardsParam(Builder builder, String shardsParam) {
    List<String> sliceOrUrls = StrUtils.splitSmart(shardsParam, ",", true);
    this.slices = new String[sliceOrUrls.size()];
    this.replicas = new List[sliceOrUrls.size()];

    ClusterState clusterState = builder.zkStateReader.getClusterState();

    for (int i = 0; i < sliceOrUrls.size(); i++) {
      String sliceOrUrl = sliceOrUrls.get(i);
      if (sliceOrUrl.indexOf('/') < 0) {
        // this is a logical shard
        this.slices[i] = sliceOrUrl;
        replicas[i] = findReplicas(builder, shardsParam, clusterState, clusterState.getCollection(builder.collection).getSlice(sliceOrUrl));
      } else {
        // this has urls
        this.replicas[i] = StrUtils.splitSmart(sliceOrUrl, "|", true);
        builder.replicaListTransformer.transform(replicas[i]);
        builder.hostChecker.checkWhitelist(clusterState, shardsParam, replicas[i]);
      }
    }
  }

  private List<String> findReplicas(Builder builder, String shardsParam, ClusterState clusterState, Slice slice) {
    if (slice == null) {
      // Treat this the same as "all servers down" for a slice, and let things continue
      // if partial results are acceptable
      return Collections.emptyList();
    } else {
      final Predicate<Replica> isShardLeader = new IsLeaderPredicate(shardStateProvider , clusterState, slice.getCollection(), slice.getName());
      Predicate<Replica> replicaPredicate = replica -> {
        return shardStateProvider.isActive(replica);
      };
      List<Replica> list = slice.getReplicas()
          .stream()
          .filter(replicaPredicate)
          .filter(replica -> !builder.onlyNrt || (replica.getType() == Replica.Type.NRT || (replica.getType() == Replica.Type.TLOG && isShardLeader.test(replica))))
          .collect(Collectors.toList());
      if(builder.replicaListTransformer != null) builder.replicaListTransformer.transform(list);
      List<String> collect = list.stream().map(Replica::getCoreUrl).collect(Collectors.toList());
      if(builder.hostChecker != null)  builder.hostChecker.checkWhitelist(clusterState, shardsParam, collect);
      return collect;
    }
  }

  private void addSlices(Map<String, Slice> target, ClusterState state, SolrParams params, String collectionName, String shardKeys, boolean multiCollection) {
    DocCollection coll = state.getCollection(collectionName);
    Collection<Slice> slices = coll.getRouter().getSearchSlices(shardKeys, params, coll);
    ClientUtils.addSlices(target, collectionName, slices, multiCollection);
  }

  @Override
  public List<String> getSliceNames() {
    return Collections.unmodifiableList(Arrays.asList(slices));
  }

  @Override
  public List<String> getReplicasBySlice(int sliceNumber) {
    assert sliceNumber >= 0 && sliceNumber < replicas.length;
    return replicas[sliceNumber];
  }

  @Override
  public int getSliceCount() {
    return slices.length;
  }

  /**
   * A predicate to test if a replica is the leader according to {@link ZkStateReader#getLeaderRetry(String, String)}.
   * <p>
   * The result of getLeaderRetry is cached in the first call so that subsequent tests are faster and do not block.
   */
  private static class IsLeaderPredicate implements Predicate<Replica> {
    private final ShardStateProvider shardStateProvider;
    private final ClusterState clusterState;
    private final String collectionName;
    private final String sliceName;
    private Replica shardLeader = null;

    public IsLeaderPredicate(ShardStateProvider shardStateProvider, ClusterState clusterState, String collectionName, String sliceName) {
      this.shardStateProvider = shardStateProvider;
      this.clusterState = clusterState;
      this.collectionName = collectionName;
      this.sliceName = sliceName;
    }

    @Override
    public boolean test(Replica replica) {
      if (shardLeader == null) {
        try {
          shardLeader = shardStateProvider.getLeader(collectionName, sliceName, -1);
        } catch (InterruptedException e) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Exception finding leader for shard " + sliceName + " in collection "
                  + collectionName, e);
        } catch (SolrException e) {
          if (log.isDebugEnabled()) {
            log.debug("Exception finding leader for shard {} in collection {}. Collection State: {}",
                sliceName, collectionName, clusterState.getCollectionOrNull(collectionName));
          }
          throw e;
        }
      }
      return replica.getName().equals(shardLeader.getName());
    }
  }

  static class Builder {
    private String collection;
    private ZkStateReader zkStateReader;
    private SolrParams params;
    private boolean onlyNrt;
    private ReplicaListTransformer replicaListTransformer;
    private HttpShardHandlerFactory.WhitelistHostChecker hostChecker;

    public Builder collection(String collection) {
      this.collection = collection;
      return this;
    }

    public Builder zkStateReader(ZkStateReader stateReader) {
      this.zkStateReader = stateReader;
      return this;
    }

    public Builder params(SolrParams params) {
      this.params = params;
      return this;
    }

    public Builder onlyNrt(boolean onlyNrt) {
      this.onlyNrt = onlyNrt;
      return this;
    }

    public Builder replicaListTransformer(ReplicaListTransformer replicaListTransformer) {
      this.replicaListTransformer = replicaListTransformer;
      return this;
    }

    public Builder whitelistHostChecker(HttpShardHandlerFactory.WhitelistHostChecker hostChecker) {
      this.hostChecker = hostChecker;
      return this;
    }

    public CloudReplicaSource build() {
      return new CloudReplicaSource(this);
    }
  }

  public static final String SLICE_ID = "_SLICE_ID";
}
