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

import java.util.List;
import java.util.Map;

import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.zookeeper.KeeperException;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;

public class MigrateSate {

  private final CoreContainer cc;

  public MigrateSate(CoreContainer cc) {
    this.cc = cc;
  }

  @EndPoint(path = "/cluster/state-migrate",
      method = SolrRequest.METHOD.POST,
      permission = PermissionNameProvider.Name.COLL_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public void migrate(PayloadObj<MigrateInfo> obj) throws Exception {
    MigrateInfo info = obj.get();
    if("#" .equals(info.collection)) return;

    DocCollection c = cc.getZkController().getZkStateReader().getCollection(info.collection);
    if (c == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such collection");
    }

    if (c.getExternalState() && info.isUpgrade) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Already external state");
    }
    if (!c.getExternalState() && !info.isUpgrade) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Not external state");
    }
    try {
      cc.getZkController().getZkStateReader().getZkClient().atomicUpdate(ZkStateReader.getCollectionPath(info.collection), bytes -> {
        Map<String, Object> json = (Map<String, Object>) Utils.fromJSON(bytes);
        Utils.setObjectByPath(json, "/" + info.collection + "/" + DocCollection.EXT_STATE,
            info.isUpgrade ? "true" : null);
        return Utils.toJSON(json);
      });
      SolrResponse rsp = cc.getCollectionsHandler().sendToOCPQueue(new ZkNodeProps(
          Overseer.QUEUE_OPERATION, RELOAD.lowerName,
          CommonParams.NAME, info.collection
      ));
      if (rsp.getException() != null) {
        throw rsp.getException();
      }

    } catch (KeeperException | InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "could not modify collection state", e);
    }


  }



  public static class MigrateInfo implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String collection;

    @JsonProperty(required = true, value = "is-upgrade")
    public boolean isUpgrade;

  }
}
