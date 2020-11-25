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

package org.apache.solr.common.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.WrappedSimpleMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.VERSION;

/**This represents the individual replica states in a collection
 * This is an immutable object
 *
 */
public class PerReplicaStates implements ReflectMapWriter {
  public static final char SEPARATOR = ':';
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @JsonProperty
  public final String path;

  @JsonProperty
  public final int cversion;

  @JsonProperty
  public final SimpleMap<State> states;

  public PerReplicaStates(String path, int cversion, List<String> states) {
    this.path = path;
    this.cversion = cversion;
    Map<String , State> tmp = new LinkedHashMap<>();

    for (String state : states) {
      State rs = State.parse(state);
      if(rs == null) continue;
      State existing = tmp.get(rs.replica);
      if (existing == null) {
        tmp.put(rs.replica, rs);
      } else {
        tmp.put(rs.replica, rs.insert(existing));
      }
    }
    this.states = new WrappedSimpleMap<>(tmp);

  }

  /**
   * Get a map of replicas and their new states
   *
   * @param newPerReplicaStates This is the list of all nodes available in ZK
   */
  public static Map<String, State> findModifiedStates(PerReplicaStates newPerReplicaStates, DocCollection coll) {
    Map<String, State> result = new HashMap<>();
    newPerReplicaStates.states.forEachEntry((replicaName, state) -> {
      Replica replica = coll.getReplica(replicaName);
      if (replica != null && replica.getReplicaState() != null && !replica.getReplicaState().asString.equals(state.asString)) {
        result.put(replicaName, state);
        //this is modified
      }
    });
    return result;
  }

  public static void persist(WriteOps ops, String parent, SolrZkClient zkClient) throws KeeperException, InterruptedException {
    try {
      persist(ops.get(), parent, zkClient);
    } catch (KeeperException.NodeExistsException | KeeperException.NoNodeException e) {
      //state is stale
      log.info("stale state for {} . retrying...", parent);
      List<Op> freshOps = ops.get(PerReplicaStates.fetch(parent, zkClient));
      persist(freshOps, parent, zkClient);
      log.info("retried for stale state {}, succeeded", parent);
    }
  }
  public static void persist(List<Op> operations, String parent, SolrZkClient zkClient) throws KeeperException, InterruptedException {
    if(operations == null || operations.isEmpty()) return;
    log.debug("Per-replica state being persisted for :{}, ops: {}", parent, operations);

    List<org.apache.zookeeper.Op> ops = new ArrayList<>(operations.size());
    for (Op op : operations) {
      //the state of the replica is being updated
      String path = parent + "/" + op.state.asString;
      List<ACL> acls = zkClient.getZkACLProvider().getACLsToAdd(path);
      ops.add(op.typ == Op.Type.ADD ?
          org.apache.zookeeper.Op.create(path, null, acls, CreateMode.PERSISTENT) :
          org.apache.zookeeper.Op.delete(path, -1));
    }
    try {
      zkClient.multi(ops, true);
      if(log.isDebugEnabled()) {
        //nocommit
        try {
          Stat stat = zkClient.exists(parent, null, true);
          log.debug("After update, cversion : {}", stat.getCversion());
        } catch (Exception e) {
        }

      }
    } catch (KeeperException e) {
      log.error("multi op exception : " + e.getMessage() + zkClient.getChildren(parent, null, true));
      throw e;
    }

  }

  public static PerReplicaStates fetch(String path, SolrZkClient zkClient) throws KeeperException, InterruptedException {
    return fetch( zkClient, new PerReplicaStates(path, -1, Collections.emptyList()));

  }

  public static PerReplicaStates fetch(SolrZkClient zkClient, PerReplicaStates current) throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(current.path, null, true);
    if(stat == null) return null;
    if(current.cversion == stat.getCversion()) return current;
    stat = new Stat();
    List<String> children = zkClient.getChildren(current.path, null,stat ,true);
    return new PerReplicaStates(current.path, stat.getCversion(), Collections.unmodifiableList(children));
  }


  private static List<Op> addDeleteStaleNodes(List<Op> ops, State rs) {
    while (rs != null) {
      ops.add(new Op(Op.Type.DELETE, rs));
      rs = rs.duplicate;
    }
    return ops;
  }

  public static String getReplicaName(String s) {
    int idx = s.indexOf(SEPARATOR);
    if (idx > 0) {
      return s.substring(0, idx);
    }
    return null;
  }

  public State get(String replica) {
    return states.get(replica);
  }

  public static class Op {
    public final Type typ;
    public final State state;

    public Op(Type typ, State replicaState) {
      if (replicaState == null) {
        new RuntimeException("replicaState==null").printStackTrace();
      }
      this.typ = typ;
      this.state = replicaState;
    }


    public enum Type {
      //add a new node
      ADD,
      //delete an existing node
      DELETE
    }

    @Override
    public String toString() {
      return typ.toString() + " : " + state;
    }
  }



  /**
   * The state of a replica as stored as a node under /collections/collection-name/state.json/replica-state
   */
  public static class State implements MapWriter {

    public final String replica;

    public final Replica.State state;

    public final Boolean isLeader;

    public final int version;

    public final String asString;

    final State duplicate;

    private State(String serialized, List<String> pieces) {
      this.asString = serialized;
      replica = pieces.get(0);
      version = Integer.parseInt(pieces.get(1));
      String encodedStatus = pieces.get(2);
      this.state = Replica.getState(encodedStatus);
      isLeader = pieces.size() > 3 && "L".equals(pieces.get(3));
      duplicate = null;
    }

    public static State parse(String serialized) {
      List<String> pieces = StrUtils.splitSmart(serialized, ':');
      if(pieces.size() < 3) return null;
      return new State(serialized, pieces);

    }

    public State(String replica, Replica.State state, Boolean isLeader, int version) {
      this(replica, state, isLeader, version, null);
    }

    public State(String replica, Replica.State state, Boolean isLeader, int version, State duplicate) {
      this.replica = replica;
      this.state = state == null ? Replica.State.ACTIVE : state;
      this.isLeader = isLeader == null ? Boolean.FALSE : isLeader;
      this.version = version;
      asString = serialize();
      this.duplicate = duplicate;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(NAME, replica);
      ew.put(VERSION, version);
      ew.put(ZkStateReader.STATE_PROP, state.toString());
      if (isLeader) ew.put(Slice.LEADER, isLeader);
      ew.putIfNotNull("duplicate", duplicate);
    }

    private State insert(State duplicate) {
      assert this.replica.equals(duplicate.replica);
      if (this.version >= duplicate.version) {
        if (this.duplicate != null) {
          duplicate = new State(duplicate.replica, duplicate.state, duplicate.isLeader, duplicate.version, this.duplicate);
        }
        return new State(this.replica, this.state, this.isLeader, this.version, duplicate);
      } else {
        return duplicate.insert(this);
      }
    }

    public List<State> getDuplicates() {
      if (duplicate == null) return Collections.emptyList();
      List<State> result = new ArrayList<>();
      State current = duplicate;
      while (current != null) {
        result.add(current);
        current = current.duplicate;
      }
      return result;
    }

    private String serialize() {
      StringBuilder sb = new StringBuilder(replica)
          .append(":")
          .append(version)
          .append(":")
          .append(state.shortName);
      if (isLeader) sb.append(":").append("L");
      return sb.toString();
    }


    @Override
    public String toString() {
      return asString;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof State) {
        State that = (State) o;
        return Objects.equals(this.asString, that.asString);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return asString.hashCode();
    }
  }


  public static abstract class WriteOps {
    private PerReplicaStates rs;
    List<Op> ops;
    private boolean preOp = true;

    public static WriteOps flipState(String replica, Replica.State newState, PerReplicaStates rs) {
      return new WriteOps() {
        @Override
        protected List<Op> refresh(PerReplicaStates rs) {
          List<Op> ops = new ArrayList<>(2);
          State existing = rs.get(replica);
          if(existing == null) {
            ops.add(new Op(Op.Type.ADD, new State(replica, newState,Boolean.FALSE, 0)));
          } else {
            ops.add(new Op(Op.Type.ADD, new State(replica, newState, existing.isLeader, existing.version + 1)));
            addDeleteStaleNodes(ops, existing);
          }
          log.debug("flipState {} -> {}, ops :{}", replica, newState, ops);
          return ops;
        }
      }.init(rs);
    }

    public PerReplicaStates getPerReplicaStates() {
      return rs;
    }

    /**Flip the leader replica to a new one
     * @param old current leader
     * @param  next next leader
     */
    public static WriteOps flipLeader(String old, String next, PerReplicaStates rs)  {
      return new WriteOps() {

        @Override
        protected List<Op> refresh(PerReplicaStates rs) {
          List<Op> ops = new ArrayList<>(4);
          if (old != null) {
            State st = rs.get(old);
            if(st != null) {
              ops.add(new Op(Op.Type.ADD, new State(st.replica, st.state, Boolean.FALSE, st.version + 1)));
              ops.add(new Op(Op.Type.DELETE, st));
            }
          }
          if(next != null) {
            State st = rs.get(next);
            if(st != null) {
              ops.add(new Op(Op.Type.ADD, new State(st.replica, Replica.State.ACTIVE, Boolean.TRUE, st.version + 1)));
              ops.add(new Op(Op.Type.DELETE, st));
            }
          }
          log.debug("flipLeader {} -> {}, ops: {}", old, next, ops);
          return ops;
        }

      }.init(rs);
    }

    /**Delete a replica entry from per-replica states
     *
     * @param replica name of the replica to be deleted
     */
    public static WriteOps deleteReplica(String replica, PerReplicaStates rs) {
      return new WriteOps() {
        @Override
        protected List<Op> refresh(PerReplicaStates rs) {
          List<Op> result;
          if(rs == null) {
            result = Collections.emptyList();
          } else {
            State state = rs.get(replica);
            result = addDeleteStaleNodes(new ArrayList<>(), state);
          }
          return result;
        }
      }.init(rs);
    }

    public static WriteOps addReplica(String replica, Replica.State state, boolean isLeader, PerReplicaStates rs) {
      return new WriteOps() {
        @Override
        protected List<Op> refresh(PerReplicaStates rs) {
          return singletonList(new Op(Op.Type.ADD,
              new State(replica, state, isLeader, 0)));
        }
      }.init(rs);
    }

    /** mark a bunch of replicas as DOWN
     */
    public static WriteOps downReplicas(List<String> replicas, PerReplicaStates rs) {
      return new WriteOps() {
        @Override
        List<Op> refresh(PerReplicaStates rs) {
          List<Op> ops = new ArrayList<>();
          for (String replica : replicas) {
            State r = rs.get(replica);
            if (r != null) {
              if (r.state == Replica.State.DOWN) continue;
              ops.add(new Op(Op.Type.ADD, new State(replica, Replica.State.DOWN, r.isLeader, r.version + 1)));
              addDeleteStaleNodes(ops, r);
            }
          }
          if(log.isDebugEnabled())
            log.debug("for coll: {} down replicas {}, ops {}",  rs.path, replicas, ops);
          return ops;
        }
      }.init(rs);
    }

    /**Just creates and deletes an entry so that the {@link Stat#getCversion()} of states.json
     * is updated
     */
    public static WriteOps touchChildren() {
      WriteOps result = new WriteOps() {
        @Override
        List<Op> refresh(PerReplicaStates rs) {
          List<Op> ops = new ArrayList<>();
          State st = new State(".dummy." + System.nanoTime(), Replica.State.DOWN, Boolean.FALSE, 0);
          ops.add(new Op(Op.Type.ADD, st));
          ops.add(new Op(Op.Type.DELETE, st));
          log.debug("touchChildren {}", ops );
          return ops;
        }
      };
      result.preOp = false;
      result.ops = result.refresh(null);
      return result;
    }

    WriteOps init(PerReplicaStates rs) {
      if(rs == null) return null;
      get(rs);
      return this;
    }

    public List<Op> get() {
      return ops;
    }

    public List<Op> get(PerReplicaStates rs) {
      ops = refresh(rs);
      if (ops == null) ops = Collections.emptyList();
      this.rs = rs;
      return ops;
    }

    /**To be executed before collection is persisted
     */
    public boolean isPreOp() {
      return preOp;
    }

    abstract List<Op> refresh(PerReplicaStates rs);

    @Override
    public String toString() {
      return ops.toString();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{").append(path).append("/[") .append(cversion).append("]: [");
    states.forEachEntry((s, state) -> {
      sb.append(state.asString).append(" , ");
      for (State d : state.getDuplicates()) {
        sb.append(d).append(" , ");
      }
    });
    return sb.append("]}").toString();
  }

}
