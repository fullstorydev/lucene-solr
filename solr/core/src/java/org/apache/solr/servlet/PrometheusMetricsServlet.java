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

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics.
 * This servlet exists for backwards compatibility and will be removed in favor of the native prometheus-exporter.
 */
public final class PrometheusMetricsServlet extends BaseSolrServlet {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final List<MetricsApiCaller> callers = Collections.unmodifiableList(Arrays.asList(
      new ThreadMetricsApiCaller(),
      new DeletesByMetricsApiCaller()
  ));

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    List<PrometheusMetric> metrics = new ArrayList<>();
    AtomicInteger qTime = new AtomicInteger();
    for(MetricsApiCaller caller : callers) {
      caller.call(qTime, metrics, request);
    }
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    PrintWriter writer = response.getWriter();
    for(PrometheusMetric metric : metrics) {
      metric.write(writer);
    }
    new PrometheusMetric("metrics_qtime", PrometheusMetricType.GAUGE, "QTime for calling metrics api", qTime).write(writer);
    writer.flush();
  }

/*
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

 */

  static class ThreadMetricsApiCaller extends MetricsApiCaller {

    ThreadMetricsApiCaller() {
      super("jvm", "threads.", "");
    }

    /*
  "metrics":{
    "solr.jvm":{
      "threads.blocked.count":0,
      "threads.count":2019,
      "threads.daemon.count":11,
      "threads.deadlock.count":0,
      "threads.deadlocks":[],
      "threads.new.count":0,
      "threads.runnable.count":16,
      "threads.terminated.count":0,
      "threads.timed_waiting.count":247,
      "threads.waiting.count":1756}}}
     */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      JsonNode parent = metrics.path("solr.jvm");
      results.add(new PrometheusMetric("threads_count", PrometheusMetricType.GAUGE, "number of threads", getNumber(parent, "threads.count")));
      results.add(new PrometheusMetric("threads_blocked_count", PrometheusMetricType.GAUGE, "number of blocked threads", getNumber(parent, "threads.blocked.count")));
      results.add(new PrometheusMetric("threads_deadlock_count", PrometheusMetricType.GAUGE, "number of deadlock threads", getNumber(parent, "threads.deadlock.count")));
      results.add(new PrometheusMetric("threads_runnable_count", PrometheusMetricType.GAUGE, "number of runnable threads", getNumber(parent, "threads.runnable.count")));
      results.add(new PrometheusMetric("threads_terminated_count", PrometheusMetricType.GAUGE, "number of terminated threads", getNumber(parent, "threads.terminated.count")));
      results.add(new PrometheusMetric("threads_timed_waiting_count", PrometheusMetricType.GAUGE, "number of timed waiting threads", getNumber(parent, "threads.timed_waiting.count")));
      results.add(new PrometheusMetric("threads_waiting_count", PrometheusMetricType.GAUGE, "number of waiting threads", getNumber(parent, "threads.waiting.count")));
    }
  }

  static class DeletesByMetricsApiCaller extends MetricsApiCaller {

    DeletesByMetricsApiCaller() {
      super("core", "UPDATE.updateHandler.cumulativeDeletesBy", "count");
    }

    /*
  "metricss":{
    "solr.core.6A7GA.shard23.replica_n23":{
      "UPDATE.updateHandler.cumulativeDeletesById":{"count":0},
      "UPDATE.updateHandler.cumulativeDeletesByQuery":{"count":0}},
    ...
   */
    @Override
    protected void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException {
      int byId = 0;
      int byQuery = 0;
      for(JsonNode core : metrics) {
        byId += getNumber(core, "UPDATE.updateHandler.cumulativeDeletesById", property).intValue();
        byQuery += getNumber(core, "UPDATE.updateHandler.cumulativeDeletesByQuery", property).intValue();
      }
      results.add(new PrometheusMetric("deletes_by_id", PrometheusMetricType.COUNTER, "cumulative number of deletes by id across cores", Integer.valueOf(byId)));
      results.add(new PrometheusMetric("deletes_by_query", PrometheusMetricType.COUNTER, "cumulative number of deletes by query across cores", Integer.valueOf(byQuery)));
    }
  }

  enum PrometheusMetricType {
    COUNTER, GAUGE
  }

  static class PrometheusMetric {

    private final String name;
    private final String type;
    private final String description;
    private final Number value;

    PrometheusMetric(String name, PrometheusMetricType type, String description, Number value) {
      this.name = name.toLowerCase().replace(" ", "_");
      this.type = type.name().toLowerCase();
      this.description = description;
      this.value = value;
    }

    void write(PrintWriter writer) throws IOException {
      writer.append("# HELP ").append(name).append(' ').append(description).println();
      writer.append("# TYPE ").append(name).append(' ').append(type).println();
      writer.append(name).append(' ').append(value.toString()).println();
    }
  }

  static Number getNumber(JsonNode node, String... names) throws IOException {
    for(String name : names) {
      node = node.path(name);
    }
    if (node.isNumber()) {
      return node.numberValue();
    } else {
      throw new IOException(String.format(Locale.ROOT, "%s is not a number value.", Arrays.toString(names)));
    }
  }

  static abstract class MetricsApiCaller {

    protected final String group;
    protected final String prefix;
    protected final String property;

    MetricsApiCaller(String group, String prefix, String property) {
      this.group = group;
      this.prefix = prefix;
      this.property = property;
    }

    void call(AtomicInteger qTime, List<PrometheusMetric> results, HttpServletRequest originalRequest) throws IOException {
      Object value = originalRequest.getAttribute(HttpSolrCall.class.getName());
      if (!(value instanceof HttpSolrCall)) {
        throw new IOException(String.format(Locale.ROOT, "request attribute %s does not exist.", HttpSolrCall.class.getName()));
      }
      HttpSolrCall originalCall = (HttpSolrCall) value;
      SolrDispatchFilter filter = originalCall.solrDispatchFilter;
      CoreContainer cores = filter.getCores();
      HttpServletRequest request = new MetricsApiRequest(originalRequest, group, prefix, property);
      MetricsApiResponse response = new MetricsApiResponse();
      SolrDispatchFilter.Action action = new HttpSolrCall(filter, cores, request, response, false).call();
      if (action != SolrDispatchFilter.Action.RETURN) {
        throw new IOException(String.format(Locale.ROOT, "metrics api call returns %s; expected %s.", action, SolrDispatchFilter.Action.RETURN));
      }
      handleResponse(qTime, results, response.getJsonNode());
    }

    void handleResponse(AtomicInteger qTime, List<PrometheusMetric> results, JsonNode response) throws IOException {
      JsonNode header = response.path("responseHeader");
      int status = getNumber(header, "status").intValue();
      if (status != 0) {
        throw new IOException(String.format(Locale.ROOT, "metrics api response status is %d; expected 0.", status));
      }
      qTime.addAndGet(getNumber(header, "QTime").intValue());
      handle(results, response.path("metrics"));
    }

    protected abstract void handle(List<PrometheusMetric> results, JsonNode metrics) throws IOException;
  }

  // represents a request to e.g., /solr/admin/metrics?wt=json&indent=false&compact=true&group=solr.jvm&prefix=memory.pools.
  // see ServletUtils.getPathAfterContext() for setting getServletPath() and getPathInfo().
  static class MetricsApiRequest extends HttpServletRequestWrapper {

    private final String queryString;
    private final Map<String, Object> attributes = new HashMap<>();

    MetricsApiRequest(HttpServletRequest request, String group, String prefix, String property) throws IOException {
      super(request);
      queryString = String.format(
          "wt=json&indent=false&compact=true&group=%s&prefix=%s&property=%s",
          URLEncoder.encode(group, StandardCharsets.UTF_8.name()),
          URLEncoder.encode(prefix, StandardCharsets.UTF_8.name()),
          URLEncoder.encode(property, StandardCharsets.UTF_8.name()));
    }

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
      return queryString;
    }

    @Override
    public Object getAttribute(String name) {
      Object value = attributes.get(name);
      if (value == null) {
        value = super.getAttribute(name);
      }
      return value;
    }

    @Override
    public Enumeration<String> getAttributeNames() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String name, Object value) {
      attributes.put(name, value);
    }

    @Override
    public void removeAttribute(String name) {
      throw new UnsupportedOperationException();
    }
  }

  static class ByteArrayServletOutputStream extends ServletOutputStream {

    private ByteArrayOutputStream output = new ByteArrayOutputStream();

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

    public byte[] getBytes() {
      return output.toByteArray();
    }
  };

  static class MetricsApiResponse implements HttpServletResponse {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private int statusCode = 0;
    private ByteArrayServletOutputStream body = new ByteArrayServletOutputStream();

    @Override
    public void setStatus(int code) {
      statusCode = code;
    }

    @Override
    public void setStatus(int code, String s) {
      statusCode = code;
    }

    @Override
    public void sendError(int code, String s) throws IOException {
      statusCode = code;
    }

    @Override
    public void sendError(int code) throws IOException {
      statusCode = code;
    }

    @Override
    public int getStatus() {
      return statusCode;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
      return body;
    }

    public JsonNode getJsonNode() throws IOException {
      if (statusCode != 0 && statusCode / 100 != 2) {
        throw new IOException(String.format(Locale.ROOT, "metrics api failed with status code %s.", statusCode));
      }
      return OBJECT_MAPPER.readTree(body.getBytes());
    }

    @Override
    public String encodeURL(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectURL(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeUrl(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectUrl(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendRedirect(String s) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addCookie(Cookie cookie) {}

    @Override
    public void setDateHeader(String s, long l) {}

    @Override
    public void addDateHeader(String s, long l) {}

    @Override
    public void setHeader(String s, String s1) {}

    @Override
    public void addHeader(String s, String s1) {}

    @Override
    public void setIntHeader(String s, int i) {}

    @Override
    public void addIntHeader(String s, int i) {}

    @Override
    public void setCharacterEncoding(String s) {}

    @Override
    public void setContentLength(int i) {}

    @Override
    public void setContentLengthLong(long l) {}

    @Override
    public void setContentType(String s) {}

    @Override
    public void setBufferSize(int i) {}

    @Override
    public void flushBuffer() throws IOException {}

    @Override
    public void resetBuffer() {}

    @Override
    public void reset() {}

    @Override
    public void setLocale(Locale locale) {}

    @Override
    public boolean containsHeader(String s) {
      return false;
    }

    @Override
    public String getHeader(String s) {
      return null;
    }

    @Override
    public Collection<String> getHeaders(String s) {
      return Collections.emptyList();
    }

    @Override
    public Collection<String> getHeaderNames() {
      return Collections.emptyList();
    }

    @Override
    public String getCharacterEncoding() {
      return StandardCharsets.UTF_8.name();
    }

    @Override
    public String getContentType() {
      return null;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getBufferSize() {
      return 0;
    }

    @Override
    public boolean isCommitted() {
      return false;
    }

    @Override
    public Locale getLocale() {
      return Locale.ROOT;
    }
  }
}