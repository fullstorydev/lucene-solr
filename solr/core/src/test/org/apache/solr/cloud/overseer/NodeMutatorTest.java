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
package org.apache.solr.cloud.overseer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NodeMutatorTest {

  @Test
  public void downNodeReportsAllImpactedCollectionsAndNothingElse() {
    NodeMutator nm = new NodeMutator(null);
    ZkNodeProps props = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, "down-node");
    // only collections A, D, and F have shard replicas on down-node
    ClusterState state = makeClusterState(map(
        "collection-a", map(
            "shard-1", map(
                "replica-1", "live-node-1",
                "replica-2", "live-node-2"
            ),
            "shard-2", map(
                "replica-1", "live-node-3",
                "replica-2", "down-node"
            )
        ),
        "collection-b", map(
            "shard-1", map(
                "replica-1", "live-node-3",
                "replica-2", "live-node-1"
            ),
            "shard-2", map(
                "replica-1", "live-node-2",
                "replica-2", "live-node-3"
            )
        ),
        "collection-c", map(
            "shard-1", map(
                "replica-1", "live-node-1",
                "replica-2", "live-node-3"
            ),
            "shard-2", map(
                "replica-1", "live-node-1",
                "replica-2", "live-node-2"
            )
        ),
        "collection-d", map(
            "shard-1", map(
                "replica-1", "live-node-3",
                "replica-2", "live-node-1"
            ),
            "shard-2", map(
                "replica-1", "down-node",
                "replica-2", "live-node-3"
            )
        ),
        "collection-e", map(
            "shard-1", map(
                "replica-1", "live-node-1",
                "replica-2", "live-node-2"
            ),
            "shard-2", map(
                "replica-1", "live-node-3",
                "replica-2", "live-node-1"
            )
        ),
        "collection-f", map(
            "shard-1", map(
                "replica-1", "down-node",
                "replica-2", "live-node-3"
            ),
            "shard-2", map(
                "replica-1", "live-node-3",
                "replica-2", "live-node-2"
            )
        )
    ));

    // invoke method under test
    List<ZkWriteCommand> writes = nm.downNode(state, props);

    // we sort it to simplify comparison of actual vs. expected
    Collections.sort(writes, new Comparator<ZkWriteCommand>() {
      @Override
      public int compare(ZkWriteCommand o1, ZkWriteCommand o2) {
        if (o1.noop && o2.noop) {
          return 0;
        }
        if (o1.noop) {
          return -1;
        } else if (o2.noop) {
          return 1;
        }
        assert o1.name != null && o2.name != null; // neither are no-ops
        return o1.name.compareTo(o2.name);
      }
    });

    // expected mutations
    state.getSlice("collection-a", "shard-2").getReplica("replica-2")
        .getProperties().put(ZkStateReader.STATE_PROP, "down");
    state.getSlice("collection-d", "shard-2").getReplica("replica-1")
        .getProperties().put(ZkStateReader.STATE_PROP, "down");
    state.getSlice("collection-f", "shard-1").getReplica("replica-1")
        .getProperties().put(ZkStateReader.STATE_PROP, "down");

    List<ZkWriteCommand> expected = Arrays.asList(
        new ZkWriteCommand("collection-a", state.getCollection("collection-a")),
        new ZkWriteCommand("collection-d", state.getCollection("collection-d")),
        new ZkWriteCommand("collection-f", state.getCollection("collection-f")));

    // NB: many of the objects in the cluster state graph do not implement equals, so we just
    // compare string representations, which include enough attributes to be correct
    assertEquals(expected.toString(), writes.toString());
  }

  private ClusterState makeClusterState(Map<String, Map<String, Map<String, String>>> structure) {
    Set<String> liveNodes = new LinkedHashSet<>();
    Map<String, DocCollection> colls = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, Map<String, String>>> collEntry : structure.entrySet()) {
      Map<String, Slice> slices = new LinkedHashMap<>();
      for (Map.Entry<String, Map<String, String>> sliceEntry : collEntry.getValue().entrySet()) {
        Map<String, Replica> replicas = new LinkedHashMap<>();
        for (Map.Entry<String, String> replEntry : sliceEntry.getValue().entrySet()) {
          String node = replEntry.getValue();
          if (node.startsWith("live")) {
            liveNodes.add(node);
          }
          Map<String, Object> props = new LinkedHashMap<>();
          props.put(ZkStateReader.NODE_NAME_PROP, node);
          replicas.put(replEntry.getKey(), new Replica(replEntry.getKey(), props));
        }
        Slice s = new Slice(sliceEntry.getKey(), replicas, Collections.<String, Object>emptyMap());
        slices.put(sliceEntry.getKey(), s);
      }
      DocCollection coll = new DocCollection(collEntry.getKey(), slices, Collections.<String, Object>emptyMap(), null);
      colls.put(collEntry.getKey(), coll);
    }
    return new ClusterState(42, liveNodes, colls);
  }

  /*
   * Wrappers for ImmutableMap building to make type inferencing happier and improve readability of code above
   */

  private static <T> Map<String, T> map(String key1, T val1, String key2, T val2) {
    return ImmutableMap.of(key1, val1, key2, val2);
  }

  private static <T> Map<String, T> map(String key1, T val1, String key2, T val2, String key3, T val3,
                                        String key4, T val4, String key5, T val5, String key6, T val6) {
    return ImmutableMap.<String, T>builder()
        .put(key1, val1)
        .put(key2, val2)
        .put(key3, val3)
        .put(key4, val4)
        .put(key5, val5)
        .put(key6, val6)
        .build();
  }
}
