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
import java.util.LinkedHashMap;
import java.util.Map;

import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
 /**
 * FullStory: a simple servlet to produce a few stats formatted for Prometheus.
 */
public final class PrometheusMetricsServlet extends BaseSolrServlet {
   public static void writePromDoc(PrintWriter writer, String name, String purpose, String type) {
     writer.printf("# HELP %s %s", name, purpose);
     writer.println();
     writer.printf("# TYPE %s %s", name, type);
     writer.println();
   }
   @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
     Writer out = null;
    try {
      response.setCharacterEncoding("UTF-8");
      response.setContentType("application/json");
       // Protect container owned streams from being closed by us, see SOLR-8933
      out = new OutputStreamWriter(new CloseShieldOutputStream(response.getOutputStream()), StandardCharsets.UTF_8);
      writeStats(new PrintWriter(out));
    } finally {
      IOUtils.closeQuietly(out);
    }
  }
   static void writeStats(PrintWriter pw) {
    Metrics statz = new Metrics();
    statz.write(pw);
    pw.flush();
  }
   static class Metrics {
    Map<String, GarbageCollectorMXBean> gc;
    MemoryUsage heap;
    MemoryUsage nonHeap;
    UnixOperatingSystemMXBean osBean;

    Metrics() {
      this.gc = new LinkedHashMap<>();
      for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
        this.gc.put(gcBean.getName(), gcBean);
      }

      MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
      this.heap = memoryBean.getHeapMemoryUsage();
      this.nonHeap = memoryBean.getNonHeapMemoryUsage();
      this.osBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    }
    
    public void write(PrintWriter writer) {
      // G1 Young Generation
      GarbageCollectorMXBean youngGcBean = this.gc.get("G1 Young Generation");
      String cname = "collection_count_young_generation";
      String ctime = "collection_time_young_generation";
      writePromDoc(writer, cname, "", "gauge");
      writer.printf("%s %d", cname, youngGcBean.getCollectionCount());
      writer.println();
      writePromDoc(writer, ctime, "", "timer");
      writer.printf("%s %d", ctime, youngGcBean.getCollectionTime());
      writer.println();

      // G1 Old Generation
      GarbageCollectorMXBean oldGcBean = this.gc.get("G1 Young Generation");
      cname = "collection_count_old_generation";
      ctime = "collection_time_old_generation";
      writePromDoc(writer, cname, "", "gauge");
      writer.printf("%s %d", cname, oldGcBean.getCollectionCount());
      writer.println();
      writePromDoc(writer, ctime, "", "timer");
      writer.printf("%s %d", ctime, oldGcBean.getCollectionTime());
      writer.println();

      // Write heap memory stats
      String cmem_name = "committed_memory_heap";
      String umem_name = "used_memory_heap";

      writePromDoc(writer, cmem_name, "", "gauge");
      writer.printf("%s %d", cmem_name, this.heap.getCommitted());
      writer.println();
      writePromDoc(writer, umem_name, "", "gauge");

      writer.printf("%s %d", umem_name, this.heap.getUsed());
      writer.println();

      // Write non_heap memory stats
      cmem_name = "committed_memory_non_heap";
      umem_name = "used_memory_non_heap";

      writePromDoc(writer, cmem_name, "", "gauge");
      writer.printf("%s %d", cmem_name, this.nonHeap.getCommitted());
      writer.println();
      writePromDoc(writer, umem_name, "", "gauge");

      writer.printf("%s %d", umem_name, this.nonHeap.getUsed());
      writer.println();

       // Write OS stats
       long openFiles = osBean.getOpenFileDescriptorCount();
       writePromDoc(writer, "open file descriptors", "", "gauge");
       writer.printf("open_file_descriptors %d", openFiles);
       writer.println();
    }


  }
}

