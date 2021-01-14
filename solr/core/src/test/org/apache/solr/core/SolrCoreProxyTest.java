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

package org.apache.solr.core;

import java.util.Random;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrCoreProxyTest extends AbstractFullDistribZkTestBase {

  public SolrCoreProxyTest() {
    sliceCount = 1;
  }

  @Before
  public void setUp() throws Exception {
    System.setProperty(CoreContainer.SOLR_QUERY_AGGREGATOR, "true");
    super.setUp();
    createQueryNode();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty(CoreContainer.SOLR_QUERY_AGGREGATOR);
  }

  @Test
  @ShardsFixed(num = 1)
  public void testCreateProxyCore() throws Exception {
    String collectionName = "SolrCoreProxyTest";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 1);

    NamedList<Object> request = create.process(cloudClient).getResponse();

    assertNotNull(request.get("success"));

    CoreContainer queryNodeContainer = null;
    for (JettySolrRunner jetty : jettys) {
      if (jetty.getCoreContainer().isQueryAggregator()) {
        queryNodeContainer = jetty.getCoreContainer();
        break;
      }
    }

    assertNotNull("There should be one query node container", queryNodeContainer);
    ClusterState clusterState = queryNodeContainer.getZkController().getClusterState();
    Set<String> queryNodes = clusterState.getLiveQueryNodes();
    Set<String> liveNodes = clusterState.getLiveNodes();

    assertTrue("Expected one but found." + queryNodes.size(), queryNodes.size() == 1);
    assertTrue("Query nodes should contain query node.", queryNodes.contains(queryNodeContainer.getZkController().getNodeName()));
    assertTrue("There must be some live nodes.", liveNodes.size() > 0);
    assertFalse("Query node should not register in live nodes.", liveNodes.contains(queryNodeContainer.getZkController().getNodeName()));


    SolrCore core = queryNodeContainer.getCore(collectionName);

    assertNotNull(core);
    assertTrue(core instanceof SolrCoreProxy);

    CoreDescriptor cd = core.getCoreDescriptor();
    assertEquals(collectionName, cd.getCollectionName());

    core.close();

    String queryNodeUrl = null;
    String ingestNodeUrl = null;
    for (JettySolrRunner jetty : jettys) {
      if (jetty.getCoreContainer().isQueryAggregator()) {
        queryNodeUrl = jetty.getBaseUrl().toString();
      } else {
        ingestNodeUrl = jetty.getBaseUrl().toString();
      }
    }
    assertNotNull(queryNodeUrl);
    assertNotNull(ingestNodeUrl);

    addDocs(ingestNodeUrl, collectionName, 10);
    //query through query node
    queryDocs(queryNodeUrl, collectionName, 10);
  }

  private void addDocs(final String baseUrl, final String collection, int docs) throws Exception {
    Random rd = new Random();
    try (HttpSolrClient qclient = getHttpSolrClient(baseUrl)) {
      for (int i = 1; i <= docs; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", rd.nextInt());
        qclient.add(collection, doc);
        qclient.commit(collection);
      }
    }
  }

  private void queryDocs(final String baseUrl, final String collection, int docs) throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    try (HttpSolrClient qclient = getHttpSolrClient(baseUrl)) {
      QueryResponse results = qclient.query(collection, query);
      assertEquals(docs, results.getResults().getNumFound());
    }
  }
}
