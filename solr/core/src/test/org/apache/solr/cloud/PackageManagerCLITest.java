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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.util.PackageTool;
import org.apache.solr.util.SolrCLI;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageManagerCLITest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static LocalWebServer repositoryServer;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("enable.packages", "true");

    configureCluster(1)
    .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
    .configure();

    repositoryServer = new LocalWebServer(TEST_PATH().resolve("question-answer-repository").toString());
    repositoryServer.start();
  }

  @AfterClass
  public static void teardown() throws Exception {
    repositoryServer.stop();
  }

  @Test
  public void testPackageManager() throws Exception {
    PackageTool tool = new PackageTool();
    String solrUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString();

    run(tool, new String[] {"-solrUrl", solrUrl, "list-installed"});

    run(tool, new String[] {"-solrUrl", solrUrl, "add-repo", "fullstory",  "http://localhost:" + repositoryServer.getPort()});

    run(tool, new String[] {"-solrUrl", solrUrl, "list-available"});

    run(tool, new String[] {"-solrUrl", solrUrl, "install", "question-answer:1.0.0"}); // no-commit (change to pkg:ver syntax)

    run(tool, new String[] {"-solrUrl", solrUrl, "list-installed"});

    CollectionAdminRequest.createCollection("abc", "conf1", 1, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("def", "conf1", 1, 1).process(cluster.getSolrClient());

    String rhPath = "/mypath2";

    run(tool, new String[] {"-solrUrl", solrUrl, "deploy", "question-answer", "-collections", "abc", "-p", "RH-HANDLER-PATH=" + rhPath});
    assertPackageVersion("abc", "question-answer", "1.0.0", rhPath, "1.0.0");

    // Should we test the "auto-update to latest" functionality or the default explicit deploy functionality
    boolean autoUpdateToLatest = random().nextBoolean();

    if (autoUpdateToLatest) {
      log.info("Testing auto-update to latest installed");

      // This command pegs the version to the latest available
      run(tool, new String[] {"-solrUrl", solrUrl, "deploy", "question-answer:latest", "-collections", "abc"});
      assertPackageVersion("abc", "question-answer", "$LATEST", rhPath, "1.0.0");

      run(tool, new String[] {"-solrUrl", solrUrl, "update", "question-answer"});
      assertPackageVersion("abc", "question-answer", "$LATEST", rhPath, "1.1.0");
    } else {
      log.info("Testing explicit deployment to a different/newer version");

      run(tool, new String[] {"-solrUrl", solrUrl, "update", "question-answer"});
      assertPackageVersion("abc", "question-answer", "1.0.0", rhPath, "1.0.0");


      if (random().nextBoolean()) { // even if parameters are not passed in, they should be picked up from previous deployment
        run(tool, new String[] {"-solrUrl", solrUrl, "deploy", "--update", "question-answer", "-collections", "abc", "-p", "RH-HANDLER-PATH=" + rhPath});
      } else {
        run(tool, new String[] {"-solrUrl", solrUrl, "deploy", "--update", "question-answer", "-collections", "abc"});
      }
      assertPackageVersion("abc", "question-answer", "1.1.0", rhPath, "1.1.0");
    }
  }

  void assertPackageVersion(String collection, String pkg, String version, String component, String componentVersion) throws Exception {
    TestSolrConfigHandler.testForResponseElement(
        null,
        cluster.getJettySolrRunner(0).getBaseUrl().toString() + "/" + collection,
        "/config/params?meta=true",
        cluster.getSolrClient(),
        Arrays.asList("response", "params", "PKG_VERSIONS", pkg),
        version,
        1);

    TestSolrConfigHandler.testForResponseElement(
        null,
        cluster.getJettySolrRunner(0).getBaseUrl().toString() + "/" + collection,
        "/config/requestHandler?componentName=" + component + "&meta=true",
        cluster.getSolrClient(),
        Arrays.asList("config", "requestHandler", component, "_packageinfo_", "version"),
        componentVersion,
        1);
  }

  private void run(PackageTool tool, String[] args) throws Exception {
    int res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    assertEquals("Non-zero status returned for: " + Arrays.toString(args), 0, res);
  }

  static class LocalWebServer {
    private int port = 0;
    final private String resourceBase;
    Server server;
    ServerConnector connector;

    public LocalWebServer(String resourceDir) {
      this.resourceBase = resourceDir;
    }

    public int getPort() {
      return connector != null? connector.getLocalPort(): port;
    }

    public LocalWebServer setPort(int port) {
      this.port = port;
      return this;
    }

    public String getResourceBase() {
      return resourceBase;
    }

    public void start() throws Exception {
      server = new Server();

      connector = new ServerConnector(server);
      connector.setPort(port);
      server.addConnector(connector);
      server.setStopAtShutdown(true);

      ResourceHandler resourceHandler = new ResourceHandler();
      resourceHandler.setResourceBase(resourceBase);
      resourceHandler.setDirectoriesListed(true);

      HandlerList handlers = new HandlerList();
      handlers.setHandlers(new Handler[] { resourceHandler, new DefaultHandler() });
      server.setHandler(handlers);

      server.start();
    }

    public void stop() throws Exception {
      server.stop();
    }
  }
}
