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

package org.apache.solr.cloud;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;

public class TestShardsWithSingleReplica extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Horrible copy-patsa.
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    final String configName = "solrCloudCollectionConfig";
    File configDir = new File(TEST_HOME() + File.separator + "collection1" + File.separator + "conf");
    configureCluster(3)
        .addConfig(configName, configDir.toPath())
        .configure();
  }

  public void testSkipLeaderOperations() throws Exception {
    String overseerLeader = getOverseerLeader();
    List<JettySolrRunner> notOverseerNodes = new ArrayList<>();
    List<String> createNodeNames = new ArrayList<>();
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      String nodeName = solrRunner.getCoreContainer().getZkController().getNodeName();
      if (!nodeName.equals(overseerLeader)) {
        notOverseerNodes.add(solrRunner);
        createNodeNames.add(nodeName);
      }
    }

    final String collection = "collection1";
    new CollectionAdminRequest.Create()
        .setCollectionName(collection)
        .setConfigName("solrCloudCollectionConfig")
        .setNumShards(2)
        .setReplicationFactor(1)
        .setCreateNodeSet(Joiner.on(',').join(createNodeNames))
        .process(cluster.getSolrClient());

    for (JettySolrRunner solrRunner : notOverseerNodes) {
      solrRunner.stop();
    }

    waitForState("Expected single liveNode", new Predicate<ClusterState>() {
      @Override
      public boolean apply(ClusterState input) {
        return input.getLiveNodes().size() == 1;
      }
    });

    CollectionAdminResponse resp = new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient());
    for (JettySolrRunner solrRunner : notOverseerNodes) {
      cluster.startJettySolrRunner(solrRunner);
    }

    waitForState("Expected 2x1 for collection: " + collection, new Predicate<ClusterState>() {
      @Override
      public boolean apply(ClusterState input) {
        Set<String> liveNodes = input.getLiveNodes();
        DocCollection collectionState = input.getCollection(collection);
        if (collectionState == null)
          return false;
        if (collectionState.getSlices().size() != 2)
          return false;
        for (Slice slice : collectionState.getSlices()) {
          int activeReplicas = 0;
          for (Replica replica : slice.getReplicas()) {
            if (liveNodes.contains(replica.getNodeName()) && replica.getState() == Replica.State.ACTIVE)
              activeReplicas++;
          }
          if (activeReplicas != 1)
            return false;
        }
        return true;
      }
    });

    CollectionAdminResponse resp2 = new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient());
    assertEquals(getNumLeaderOpeations(resp), getNumLeaderOpeations(resp2));
  }

  private int getNumLeaderOpeations(CollectionAdminResponse resp) {
    return (int) resp.getResponse().findRecursive("overseer_operations", "leader", "requests");
  }

  private String getOverseerLeader() throws IOException, SolrServerException {
    CollectionAdminResponse resp = new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient());
    return (String) resp.getResponse().get("leader");
  }

  private static void waitForState(String message, Predicate<ClusterState> pred) throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      if (pred.apply(cluster.getSolrClient().getZkStateReader().getClusterState())) {
        return;
      }
      Thread.sleep(200);
    }
    fail(message);
  }
}
