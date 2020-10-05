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
package org.apache.solr.servlet;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics.
 * This servlet exists for backwards compatibility and will be removed in favor of the native prometheus-exporter.
 */
public final class PrometheusMetricsServlet extends BaseSolrServlet {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private enum PromType {
    counter,
    gauge
  }

  private static void writeProm(PrintWriter writer, String inName, PromType type, String desc, long value) {
    String name = inName.toLowerCase().replace(" ", "_");
    writer.printf("# HELP %s %s", name, desc);
    writer.println();
    writer.printf("# TYPE %s %s", name, type);
    writer.println();
    writer.printf("%s %d", name, value);
    writer.println();
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

    try {
      scrapeMetricsApi(request, response, "&group=solr.jvm&prefix=memory.pools", null);
    } catch (Exception e) {
      e.printStackTrace();
    }

    Writer out = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");
    PrintWriter pw = new PrintWriter(out);
    writeStats(pw, (CoreContainer) request.getAttribute(CoreContainer.class.getName()));
  }

  static void writeStats(PrintWriter writer, CoreContainer coreContainer) {
    // GC stats
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      writeProm(writer, "collection_count_" + gcBean.getName(), PromType.counter, "the number of GC invocations for " + gcBean.getName(), gcBean.getCollectionCount());
      writeProm(writer, "collection_time_" + gcBean.getName(), PromType.counter, "the total number of milliseconds of time spent in gc for " + gcBean.getName(), gcBean.getCollectionTime());
    }

    // Write heap memory stats
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    MemoryUsage heap = memoryBean.getHeapMemoryUsage();
    writeProm(writer, "committed_memory_heap", PromType.gauge, "amount of memory in bytes that is committed for the Java virtual machine to use in the heap", heap.getCommitted());
    writeProm(writer, "used_memory_heap", PromType.gauge, "amount of used memory in bytes in the heap", heap.getUsed());


    MemoryUsage nonHeap = memoryBean.getNonHeapMemoryUsage();
    writeProm(writer, "committed_memory_nonheap", PromType.gauge, "amount of memory in bytes that is committed for the Java virtual machine to use in the heap", nonHeap.getCommitted());
    writeProm(writer, "used_memory_nonheap", PromType.gauge, "amount of used memory in bytes in the heap", nonHeap.getUsed());

    // Write OS stats
    UnixOperatingSystemMXBean osBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    writeProm(writer, "open_file_descriptors", PromType.gauge, "the number of open file descriptors on the filesystem", osBean.getOpenFileDescriptorCount());

    writeCacheMetrics(writer, coreContainer);
    writeExtraMetrics(writer);
    writer.flush();
  }

  private static void writeExtraMetrics(PrintWriter writer) {
    writeProm(writer, "solr_request_count", PromType.counter, "number of requests received by solr", 73);
    writeProm(writer, "solr_G1-Eden-Space_size", PromType.counter, "size of eden space in bytes", 703594496);
    writeProm(writer, "solr_G1-Eden-Space_used", PromType.counter, "used eden space in bytes", 36700160);
    writeProm(writer, "solr_G1-Old-Gen_size", PromType.counter, "size of old gen in bytes", 1422917632);
    writeProm(writer, "solr_G1-Old-Gen_used", PromType.counter, "used old gen in bytes", 22624256);
    writeProm(writer, "solr_G1-Survivor-Space_size", PromType.counter, "size of survivor space in bytes", 20971520);
    writeProm(writer, "solr_G1-Survivor-Space_used", PromType.counter, "used survivor space in bytes", 20971520);
    writeProm(writer, "solr_thread_runnable_count", PromType.counter, "number of runnable threads", 16);
    writeProm(writer, "solr_thread_terminated_count", PromType.counter, "number of terminated threads", 0);
    writeProm(writer, "solr_thread_timed_waiting_count", PromType.counter, "number of timed waiting threads", 14);
    writeProm(writer, "solr_thread_waiting_count", PromType.counter, "number of waiting threads", 13);
    writeProm(writer, "solr_delete_by_query_count", PromType.counter, "number of deletes by query across cores", 0);
    writeProm(writer, "solr_delete_by_id_count", PromType.counter, "number of deletes by id across cores", 0);
  }

  private static void writeCacheMetrics(PrintWriter writer, CoreContainer coreContainer) {
    if (coreContainer == null || coreContainer.getZkController() == null ) {
      return;
    }
    Supplier<Map> supplier = (Supplier<Map>) coreContainer.getZkController().getSolrCloudManager().getObjectCache().get(SHARED_CACHE_METRIC_NAME);
    if (supplier == null) {
      return;
    }
    Map<String, NamedList> cacheStats = supplier.get();
    if (cacheStats != null) {
      cacheStats.forEach((cacheName, namedList) -> {
        namedList.forEach((BiConsumer<String, Object>) (statName, v) -> {
          if (v instanceof Number) {
            Number number = (Number) v;
            writeProm(writer,
                "cache."+cacheName + "."+ statName,
                PromType.gauge,
                "cache info:" + statName,
                number.longValue());
          }
        });
      });
    }
  }
  public static final String SHARED_CACHE_METRIC_NAME =  "fs-shared-caches";

  static class PromEntry {

    private final String name;
    private final PromType type;
    private final String description;
    private final Number value;

    public PromEntry(String name, PromType type, String description, Number value) {
      this.name = name;
      this.type = type;
      this.description = description;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public PromType getType() {
      return type;
    }

    public String getDescription() {
      return description;
    }

    public Number getValue() {
      return value;
    }
  }

  @FunctionalInterface
  static interface ScrapeResponseHandler {
    void handle(List<PromEntry> result, JsonNode metrics) throws Exception;
  }

  void scrapeMetricsApi(HttpServletRequest oldRequest, HttpServletResponse oldResponse, String queryString, ScrapeResponseHandler handler) throws Exception {
    HttpSolrCall oldCall = (HttpSolrCall) oldRequest.getAttribute(HttpSolrCall.class.getName());
    SolrDispatchFilter filter = oldCall.getSolrDispatchFilter();
    CoreContainer cores = filter.getCores();
    final HttpServletRequest newRequest = new HttpServletRequestWrapper(oldRequest) {

      @Override
      public String getServletPath() {
        return CommonParams.METRICS_PATH;
      }

      @Override
      public String getPathInfo() {
        return null;
      }

      @Override
      public String getQueryString() {
        return "group=solr.jvm&prefix=threads";
      }
    };
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final HttpServletResponse newResponse = new HttpServletResponseWrapper(oldResponse) {
      @Override
      public ServletOutputStream getOutputStream() throws IOException {
        return new ServletOutputStream() {

          @Override
          public void write(int b) throws IOException {
            output.write(b);
          }

          @Override
          public boolean isReady() {
            return true;
          }

          @Override
          public void setWriteListener(WriteListener writeListener) {}
        };
      }
    };
    HttpSolrCall call = new HttpSolrCall(filter, cores, newRequest, newResponse, false);
    Object result = call.call();
    result.hashCode();
    String json = output.toString(StandardCharsets.UTF_8.name());
    json.hashCode();
  }
}