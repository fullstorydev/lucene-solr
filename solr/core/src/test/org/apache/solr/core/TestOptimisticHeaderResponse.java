package org.apache.solr.core;

import org.apache.http.client.methods.HttpGet;
import org.apache.solr.client.solrj.cloud.OptimisticShardState;
import org.apache.solr.client.solrj.cloud.OptimisticShardState.ExpectedType;
import org.apache.solr.client.solrj.cloud.ShardStateProvider;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class TestOptimisticHeaderResponse extends SolrCloudTestCase {
  private static final String COLLECTION = "testOptimisticHeader";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 4)
        .setExternalState(true)
        .process(cluster.getSolrClient());
    waitForState("", COLLECTION, clusterShape(1, 4));
  }


  @Test
  public void test() throws Exception {
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
    // now let's write the data on some of these nodes a few times so that the version is updated
    for (int i = 0; i < 10; i++) {
      //we update the same data again and again so that the version is updated
      cluster.getZkClient().atomicUpdate(ZkStateReader.getCollectionPath(COLLECTION), bytes -> bytes);
      cluster.getZkClient().atomicUpdate(ZkStateReader.COLLECTIONS_ZKNODE + "/" + COLLECTION + "/terms/shard1", bytes -> bytes);
    }

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
}
