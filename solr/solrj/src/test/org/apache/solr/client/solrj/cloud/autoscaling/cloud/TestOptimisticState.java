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

package org.apache.solr.client.solrj.cloud.autoscaling.cloud;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.OptimisticState;
import org.apache.solr.common.util.Utils;

public class TestOptimisticState  extends SolrTestCaseJ4 {

  public void testHeaderVal() {
    OptimisticState state = new OptimisticState();
    state.collection = new OptimisticState.Entry("mycollection", 123);
    state.shards = ImmutableList.of(new OptimisticState.Entry("shard1", 345),
        new OptimisticState.Entry("shard2", 789));
    String stateSerialized = OptimisticState.serialize(Collections.singletonList(state));
    assertEquals("{mycollection:123,shard1:345,shard2:789}", stateSerialized);


    OptimisticState out = new OptimisticState((Map) Utils.fromJSONString(stateSerialized));
    assertEquals("mycollection",out.collection.name );
    assertEquals(123,out.collection.version);
    assertEquals("shard1",out.shards.get(0).name );
    assertEquals(345,out.shards.get(0).version);
    assertEquals("shard2",out.shards.get(1).name );
    assertEquals(789,out.shards.get(1).version);


  }
}
