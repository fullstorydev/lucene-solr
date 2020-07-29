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

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

public class OptimisticShardState implements MapWriter {
  private final Builder builder;

  private OptimisticShardState(Builder  builder) {
    this.builder = builder;

  }
  public Status status() {
    return builder.status;
  }

  public ExpectedType expectedType(){
    return builder.type;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    builder.writeMap(ew);
  }

  public String getSliceName() {
    return builder.getSliceName();
  }
  public int getSliceVersion(String slice, int def) {
    return builder.getSliceVersion(slice, def);

  }

  public String getCollection() {
    return builder.collection.name;
  }
  public int getCollectionVer() {
    return builder.collection.val;
  }
  public List<Entry> slices(){
    return builder.slices();
  }



  public static class Builder implements MapWriter {
    public Status status = Status.STATUS_DEF;
    Entry collection;
    //-1 default value, ignore
    // 0 may or  may not have the leader
    // 1 expects this node to have the leader
    ExpectedType type = ExpectedType.NONE;
    Entry slice;
    List<Entry> slices;
    public Builder withCollection(String collection, int val ){
      this.collection = new Entry(collection, val);
      return this;
    }

    private String getSliceName() {
      if (slice != null) return slice.name;
      return null;
    }

    private int getSliceVersion(String slice, int def) {
      if (slice == null) {
        if (this.slice != null) return this.slice.val;
        return def;
      }
      if (this.slice != null && this.slice.name.equals(slice)) {
        return def;
      }
      if (slices == null) return def;

      for (Entry alice : slices) {
        if (alice.name.equals(slice)) return alice.val;

      }
      return def;

    }

    public Builder withExpectedType(ExpectedType type){
      this.type = type;
      return this;
    }


    @SuppressWarnings("unchecked")
    public Builder withData(String data){
      Map<String, Number> vals = (Map<String, Number>) Utils.fromJSONString(data);
      slices = new ArrayList<>(vals.size() - 1);
      vals.forEach((s, number) -> {
        if (STATUS.equals(s)) {
          status = Status.get(number.intValue());
          return;
        }
        if (TYPE.equals(s)) {
          type = ExpectedType.get(number.intValue());
          return;
        }
        if (collection == null) collection = new Entry(s, number.intValue());
        else addSlice(s, number.intValue());
      });
      return this;
    }

    public Builder addSlice(String slice, int ver) {
      Entry e = new Entry(slice, ver);
      if (this.slice == null) {
        this.slice = e;
        return this;
      }
      if (slices == null) slices = new LinkedList<>();

      slices.add(e);
      return this;
    }

    private List<Entry> slices() {
      if(slice == null) return Collections.emptyList();
      if(slices == null) return Collections.singletonList(slice);
      ArrayList<Entry> entries = new ArrayList<>(slices.size()+1);
      entries.add(slice);
      entries.addAll(slices);
      return entries;
    }


    public OptimisticShardState build() {
      return new OptimisticShardState(this);
    }
    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      if (status != Status.STATUS_DEF) ew.put(STATUS, status.val);
      if (type != ExpectedType.NONE) {
        ew.put(TYPE, type.val);
      }
      collection.writeMap(ew);
      if (slice != null) slice.writeMap(ew);
      if (slices == null) return;
      for (Entry shard : slices) shard.writeMap(ew);
    }
  }



  public static class Entry implements MapWriter {
    public final String name;
    /**
     * the znode version in the local node
     * '-1' means we currently do not know the value.
     * Maybe we are not watching that node. The receiver can take
     * an appropriate action
     */
    public final int val;

    public Entry(String name, int val) {
      this.name = name;
      this.val = val;
    }


    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(name, val);
    }
  }

  public static String serialize(OptimisticShardState state) {
    try {
      return Utils.writeJson(state, new StringWriter(), false).toString();
    } catch (IOException e) {
      //writing to StringWriter. Exception is unlikely
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return serialize(Collections.singletonList(this));
  }

  public static String serialize(List<OptimisticShardState> l) {
    if (l == null || l.size() == 0) return null;
    try {
      if (l.size() == 1) {
        return Utils.writeJson(l.get(0), new StringWriter(), false).toString();
      } else {
        return Utils.writeJson(l, new StringWriter(), false).toString();
      }
    } catch (IOException e) {
      //writing to StringWriter. Exception is unlikely
      throw new RuntimeException(e);
    }
  }

  public static final String STATUS = "@S";
  public static final String TYPE = "@T";
  public static final String STATE_HDR = "solr-state-hdr";

  public enum Status {
    /**
     * All good. nothing to do.
     * Most likely, this is not sent
     */
    STATUS_DEF(0),
    /**
     * Could not execute request because assumptions were wrong.
     * Update state and retry with fresh state
     */
    STATUS_FAIL_UPDATE(1),
    /**
     * This request is executed successfully, but the cached state versions of some are wrong
     * The expected versions are in the other attributes
     */
    STATUS_OK_UPDATE(2);

    public final int val;

    Status(int val) {
      this.val = val;
    }

    public static Status get(int v) {
      for (Status s : Status.values()) {
        if (s.val == v) return s;
      }
      return STATUS_DEF;
    }
  }

  public enum ExpectedType {
    NONE(0), LEADER(1), NRT(2), TLOG(3), PULL(4);

    public final int val;

    ExpectedType(int val) {
      this.val = val;
    }

    public static ExpectedType get(int v) {
      for (ExpectedType t : values()) {
        if (t.val == v) return t;
      }
      return ExpectedType.NONE;
    }
  }

}