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

import org.apache.http.client.methods.HttpGet;
import org.apache.solr.client.solrj.cloud.OptimisticShardState;
import org.apache.solr.client.solrj.cloud.OptimisticShardState.ExpectedType;
import org.apache.solr.client.solrj.cloud.ShardStateProvider;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class TestOptimisticHeaderResponse extends SolrCloudTestCase {
  private static final String COLLECTION = "testOptimisticHeader";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(6)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 4)
        .setExternalState(true)
        .process(cluster.getSolrClient());
    waitForState("", COLLECTION, clusterShape(1, 4));
  }


  @Test
  public void testServerFoResponse() throws Exception {
    ClusterStateProvider clusterStateProvider = cluster.getSolrClient().getClusterStateProvider();
    DocCollection coll = clusterStateProvider.getCollection(COLLECTION);
    assertTrue(coll.getExternalState());
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    ShardStateProvider ssp = zkStateReader.getShardStateProvider(COLLECTION);


    Set<String> nodesWithColl = new HashSet<>();
    coll.forEachReplica((s, replica) -> nodesWithColl.add(replica.getNodeName()));
    JettySolrRunner jettyWithoutColl = null;
    for (JettySolrRunner j : cluster.getJettySolrRunners()) {
      if (!nodesWithColl.contains(j.getNodeName())) jettyWithoutColl = j;
    }
    if (jettyWithoutColl == null) {
      fail("No node found without that collection");
    }
    updateAllStates();

    try (HttpSolrClient cl = (HttpSolrClient) jettyWithoutColl.newClient()) {
      OptimisticShardState state = new OptimisticShardState.Builder()
          .withCollection(COLLECTION, 0)
          .addSlice("shard1", 0).build();
      String uri = jettyWithoutColl.getBaseUrl().toString() + "/" + COLLECTION + "/select?q=*:*";
      HttpGet get = new HttpGet(uri);
      get.addHeader(OptimisticShardState.STATE_HDR, state.toString());
      JSONConsumer consumer = new JSONConsumer();
      Utils.execute(cl.getHttpClient(), uri, consumer, get);
      assertNotNull(consumer.respHeader);

      //test the same with V2 API
      uri = jettyWithoutColl.getBaseURLV2().toString() + "/c/" + COLLECTION + "/select?q=*:*";
      get = new HttpGet(uri);
      get.addHeader(OptimisticShardState.STATE_HDR, state.toString());
      consumer = new JSONConsumer();
      Utils.execute(cl.getHttpClient(), uri, consumer, get);
      assertNotNull(consumer.respHeader);
    }
    Replica leader = ssp.getLeader(COLLECTION, "shard1");

    //let's find a node that has the collection , but not the leader
    JettySolrRunner nonLeaderNode = null;
    for (String s : nodesWithColl) {
      if (!s.equals(leader.getNodeName())) {
        for (JettySolrRunner j : cluster.getJettySolrRunners()) {
          if (j.getNodeName().equals(s)) {
            nonLeaderNode = j;
            break;
          }
        }
        break;
      }
    }

    assertNotNull(nonLeaderNode);
    //this node has  a replica
    try (HttpSolrClient cl = (HttpSolrClient) nonLeaderNode.newClient()) {
      OptimisticShardState state = new OptimisticShardState.Builder()
          .withCollection(COLLECTION, 0)
          .withExpectedType(ExpectedType.LEADER)
          .addSlice("shard1", 0).build();
      String uri = nonLeaderNode.getBaseUrl().toString() + "/" + COLLECTION + "/select?q=*:*";

      HttpGet get = new HttpGet(uri);
      get.addHeader(OptimisticShardState.STATE_HDR, state.toString());
      JSONConsumer consumer = new JSONConsumer();
      Utils.execute(cl.getHttpClient(), uri, consumer, get);
      assertNotNull(consumer.respHeader);

      //test with V2 API

      uri = nonLeaderNode.getBaseURLV2().toString() + "/c/" + COLLECTION + "/select?q=*:*";
      get = new HttpGet(uri);
      get.addHeader(OptimisticShardState.STATE_HDR, state.toString());
      consumer = new JSONConsumer();
      Utils.execute(cl.getHttpClient(), uri, consumer, get);
      assertNotNull(consumer.respHeader);


    }
  }

  private void updateAllStates() throws KeeperException, InterruptedException {
    // now let's write the data on some of these nodes a few times so that the version is updated
    for (int i = 0; i < 10; i++) {
      //we update the same data again and again so that the version is updated
      cluster.getZkClient().atomicUpdate(ZkStateReader.getCollectionPath(COLLECTION), bytes -> bytes);
      cluster.getZkClient().atomicUpdate(ZkStateReader.COLLECTIONS_ZKNODE + "/" + COLLECTION + "/terms/shard1", bytes -> bytes);
    }
  }

  static class JSONConsumer implements Utils.InputStreamConsumer<Map<String,Object>>, Utils.HeaderConsumer {
    public OptimisticShardState respHeader;

    @Override
    @SuppressWarnings("unchecked")
    public Map<String,Object> accept(InputStream is) throws IOException {
      return (Map<String, Object>) Utils.fromJSON(is);
    }

    @Override
    public boolean readHeader(int status, Function<String, String> headerProvider) {
      String s = headerProvider.apply(OptimisticShardState.STATE_HDR);
      if (s != null) {
        respHeader = new OptimisticShardState.Builder().withData(s).build();
      }
      return status != 410;
    }

  }

  public void testHttpShardHandlerFactory() throws Exception {
    //collect all nodes that do not serve this collection
    ClusterStateProvider clusterStateProvider = cluster.getSolrClient().getClusterStateProvider();
    DocCollection coll = clusterStateProvider.getCollection(COLLECTION);



    Set<String> nodesWithColl = new HashSet<>();
    coll.forEachReplica((s, replica) -> nodesWithColl.add(replica.getNodeName()));

    List<JettySolrRunner> jettyWithoutColl = new ArrayList<>();

    List<String> wrongurls = new ArrayList<>();
    for (JettySolrRunner j : cluster.getJettySolrRunners()) {
      if (!nodesWithColl.contains(j.getNodeName())) {
        jettyWithoutColl.add(j);
        wrongurls.add(j.getBaseUrl()+"/shard1");//does not matter
      }
    }

    try(HttpShardHandlerFactory hshf = new HttpShardHandlerFactory()){
      hshf.init(ShardHandlerFactory.DEFAULT_SHARDHANDLER_INFO);
      SolrRequestInfo info = new SolrRequestInfo((SolrQueryRequest) new LocalSolrQueryRequest((SolrCore)null, (SolrParams) null), new SolrQueryResponse());
      ResponseBuilder rb = new ResponseBuilder(null, null, Collections.emptyList());
      updateAllStates();

      CloudReplicaSource replicaSource = new CloudReplicaSource.Builder()
          .zkStateReader(cluster.getSolrClient().getZkStateReader())
          .params(new ModifiableSolrParams())
          .collection(COLLECTION)
          .build();
      DelegatingShardStateAwareReplicaSource delegatingShardStateAwareReplicaSource = new DelegatingShardStateAwareReplicaSource(replicaSource,
          cluster.getSolrClient().getZkStateReader());
      delegatingShardStateAwareReplicaSource.wrongShardUrls = wrongurls;
      rb.replicaSource = delegatingShardStateAwareReplicaSource;
      rb.slices = replicaSource.getSliceNames().toArray(new String[replicaSource.getSliceCount()]);
      rb.shards = new String[rb.slices.length];
      for (int i = 0; i < rb.slices.length; i++) {
        rb.shards[i] = StrUtils.join(replicaSource.getReplicasBySlice(0), '|');
      }
      info.setResponseBuilder(rb);


      SolrRequestInfo.setRequestInfo(info);
      ShardRequest sreq = new ShardRequest();
      sreq.actualShards = rb.shards;
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add("q", "*:*");
      solrParams.add("distrib", "false");
      solrParams.add("distrib", "false");
      solrParams.add(CloudReplicaSource.SLICE_ID, String.valueOf(0));
      ShardHandler shardHandler = hshf.getShardHandler();
      shardHandler.submit(sreq,
          StrUtils.join(replicaSource.getReplicasBySlice(0), '|'),
          solrParams);
      ShardResponse completed = shardHandler.takeCompletedOrError();
      SolrDocumentList sdl = (SolrDocumentList) completed.getSolrResponse().getResponse().get("response");
      assertNotNull(sdl);
      assertEquals(0, sdl.getNumFound());

      assertTrue(delegatingShardStateAwareReplicaSource.refreshStateInvoked);
      System.out.println();

    }
  }
  static class DelegatingShardStateAwareReplicaSource implements ShardStateAwareReplicaSource {
    final ShardStateAwareReplicaSource delegate;
    boolean refreshStateInvoked = false;
    List<String> wrongShardUrls ;
    ZkStateReader reader;

    DelegatingShardStateAwareReplicaSource(ShardStateAwareReplicaSource delegate, ZkStateReader reader) {
      this.delegate = delegate;
      this.reader = reader;
    }


    @Override
    public ShardStateAwareReplicaSource refreshState(OptimisticShardState oss) {
      refreshStateInvoked = true;
      return this;
    }

    @Override
    public OptimisticShardState getOptimisticShardState(int sliceIdx) {
      int colVer=0, shardver=0;
      if (refreshStateInvoked) {
        colVer = reader.getCollection(COLLECTION).getZNodeVersion();
        shardver = reader.getShardTermsStateProvider().getTermsData(COLLECTION, "shard1", -1).getVersion();
      }
      return new OptimisticShardState.Builder()
          .withCollection(COLLECTION, colVer)
          .addSlice("shard1", shardver)
          .build();
    }

    @Override
    public int getSliceId(String shard) {
      //we only have shard
      return 0;
    }

    @Override
    public ShardStateProvider getShardStateProvider() {
     return  delegate.getShardStateProvider();
    }

    @Override
    public List<String> getSliceNames() {
      return delegate.getSliceNames();
    }

    @Override
    public List<String> getReplicasBySlice(int sliceNumber) {
      if(!refreshStateInvoked) {
        return wrongShardUrls;
      }
      return delegate.getReplicasBySlice(sliceNumber);
    }

    @Override
    public int getSliceCount() {
      return delegate.getSliceCount();
    }


  }
}
