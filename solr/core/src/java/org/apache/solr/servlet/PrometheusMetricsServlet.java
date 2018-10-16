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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;

import com.sun.management.UnixOperatingSystemMXBean;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics.
 */
public final class PrometheusMetricsServlet extends BaseSolrServlet {
  private static void writePromDoc(PrintWriter writer, String name, String purpose, String type) {
    writer.printf("# HELP %s %s", name, purpose);
    writer.println();
    writer.printf("# TYPE %s %s", name, type);
    writer.println();
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    Writer out = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");
    PrintWriter pw = new PrintWriter(out);
    writeStats(pw);
  }

  static void writeStats(PrintWriter writer) {
    // GC stats
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      String name = gcBean.getName().toLowerCase().replace(" ", "_");
      String cname = "collection_count_" + name;
      String ctime = "collection_time_" + name;
      writePromDoc(writer, cname, gcBean.getName() + " collection count", "gauge");
      writer.printf("%s %d", cname, gcBean.getCollectionCount());
      writer.println();
      writePromDoc(writer, ctime, gcBean.getName() + " collection time", "timer");
      writer.printf("%s %d", ctime, gcBean.getCollectionTime());
      writer.println();
    }

    // Write heap memory stats
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heap = memoryBean.getHeapMemoryUsage();
    MemoryUsage nonHeap = memoryBean.getNonHeapMemoryUsage();

    String cmem_name = "committed_memory_heap";
    String umem_name = "used_memory_heap";

    writePromDoc(writer, cmem_name, "committed memory heap", "gauge");
    writer.printf("%s %d", cmem_name, heap.getCommitted());
    writer.println();
    writePromDoc(writer, umem_name, "used memory heap", "gauge");

    writer.printf("%s %d", umem_name, heap.getUsed());
    writer.println();

    // Write non_heap memory stats
    cmem_name = "committed_memory_non_heap";
    umem_name = "used_memory_non_heap";

    writePromDoc(writer, cmem_name, "committed memory non-heap", "gauge");
    writer.printf("%s %d", cmem_name, nonHeap.getCommitted());
    writer.println();
    writePromDoc(writer, umem_name, "used memory non-heap", "gauge");

    writer.printf("%s %d", umem_name, nonHeap.getUsed());
    writer.println();

    // Write OS stats
    UnixOperatingSystemMXBean osBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    long openFiles = osBean.getOpenFileDescriptorCount();
    writePromDoc(writer, "open file descriptors", "open file descriptors", "gauge");
    writer.printf("open_file_descriptors %d", openFiles);
    writer.println();

    writer.flush();
  }
}
