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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

public class OptimisticState implements MapWriter {
  public Entry collection;
  public List<Entry> shards;

  public OptimisticState() {

  }

  public OptimisticState(Map<String, Number> vals) {
    shards = new ArrayList<>(vals.size() - 1);
    vals.forEach((s, number) -> {
      if (collection == null) collection = new Entry(s, number.intValue());
      else shards.add(new Entry(s, number.intValue()));
    });
  }


  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    collection.writeMap(ew);
    if (shards == null) return;
    for (Entry shard : shards) shard.writeMap(ew);
  }

  public static class Entry implements MapWriter {
    public final String name;
    public final int version;

    public Entry(String name, int version) {
      this.name = name;
      this.version = version;
    }


    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(name, version);
    }
  }

  public static String serialize(List<OptimisticState> l) {
    if (l == null || l.size() == 0) return null;
    try {
      if (l.size() == 1) {
        return Utils.writeJson(l.get(0), null, false, true).toString();
      } else {
        return Utils.writeJson(l, null, false, true).toString();
      }
    } catch (IOException e) {
      //writing to StringWriter. Exception is unlikely
      throw new RuntimeException(e);
    }
  }

}
