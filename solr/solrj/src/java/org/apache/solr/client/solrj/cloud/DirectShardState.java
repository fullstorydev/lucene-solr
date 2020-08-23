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

import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;

/**Reads the state from state.json
 *
 */
public class DirectShardState implements ShardStateProvider {

  private final Predicate<String> isNodeLive;
  private final Function<String , DocCollection> collectionProvider;

  public DirectShardState(Predicate<String> isNodeLive, Function<String, DocCollection> collectionProvider) {
    this.isNodeLive = isNodeLive;
    this.collectionProvider = collectionProvider;
  }

  @Override
  public Replica.State getState(String collection, String shard, String replica) {
    DocCollection c = collectionProvider.apply(collection);
    if(c == null) return null;
    Slice s = c.getSlice(shard);
    if(s == null) return null;
    Replica r = s.getReplica(replica);
    if(r == null) return null;
    return r.getState();
  }

  @Override
  public Replica.State getState(Replica replica) {
    return replica.getState();
  }

  @Override
  public Replica getLeader(Slice slice) {
    if(slice == null) return null;
    return slice.getLeader();
  }

  @Override
  public Replica getLeader(String coll, String slice) {
    DocCollection c = collectionProvider.apply(coll);
    if (c == null) return null;
    return getLeader(c.getSlice(slice));
  }

  @Override
  public boolean isActive(Replica replica) {
    return  replica.getNodeName() != null &&
        replica.getState() == Replica.State.ACTIVE &&
        isNodeLive.test(replica.getNodeName());
  }

  @Override
  public boolean isActive(Slice slice) {
    return slice.getState() == Slice.State.ACTIVE;
  }

  @Override
  public Replica getLeader(Slice slice, int timeout) throws InterruptedException {
    throw new RuntimeException("Not implemented");//TODO
  }

  @Override
  public Replica getLeader(String collection, String slice, int timeout) throws InterruptedException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean isExternalState() {
    return false;
  }
}
