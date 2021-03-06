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

package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.pkg.PackagePluginHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrCacheHolder<K, V> implements SolrCache<K, V> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected volatile SolrCache<K, V> delegate;
  private final PackagePluginHolder holder;

  public SolrCacheHolder(CacheConfig factory, SolrCore core) {
    holder = new PackagePluginHolder<SolrCache>(new PluginInfo("cache", (Map) factory.args), core,
        SolrConfig.classVsSolrPluginInfo.get(SolrCache.class.getName())) {
      @Override
      protected void reload(PackageLoader.Package pkg) {
        super.reload(pkg);
        delegate = (SolrCache) get();
        factory.initCache(delegate);
        if (metricsInitializer != null) metricsInitializer.run();
      }
    };
    SolrCache<K, V> inst = (SolrCache<K, V>) holder.get();
    factory.initCache(inst);
    this.delegate = inst;
  }

  public int size() {
    return delegate.size();
  }

  public V put(K key, V value) {
    return delegate.put(key, value);

  }

  public V get(K key) {
    return delegate.get(key);
  }

  public void clear() {
    delegate.clear();
  }

  @Override
  public void setState(State state) {
    delegate.setState(state);
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    if (old instanceof SolrCacheHolder) old = ((SolrCacheHolder) old).get();
    delegate.warm(searcher, old);

  }

  public SolrCache<K, V> get() {
    return delegate;
  }

  public void close() {
    delegate.close();
  }


  public void warm(SolrIndexSearcher searcher, SolrCacheHolder src) {
    delegate.warm(searcher, src.get());
  }

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    return null;
  }

  @Override
  public String name() {
    return delegate.name();
  }


  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return delegate.getMetricRegistry();
  }

  @Override
  public Set<String> getMetricNames() {
    return delegate.getMetricNames();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public Category getCategory() {
    return delegate.getCategory();
  }

  private Runnable metricsInitializer = null;

  @Override
  public void initializeMetrics(SolrMetricManager manager,
                                String registry, String tag, String scope) {
    metricsInitializer = () -> delegate.initializeMetrics(manager, registry, tag, scope);
    metricsInitializer.run();

  }
}
