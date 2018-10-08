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
public final class PrometheusStatzServlet extends BaseSolrServlet {
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
    Statz statz = new Statz();
    statz.write(pw);
    pw.flush();
  }
   static class Statz {
    Map<String, GcStatz> gc;
    HeapStatz heap;
    HeapStatz nonHeap;
    OsStatz os;
     Statz() {
      this.gc = new LinkedHashMap<>();
      for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
        this.gc.put(gcBean.getName(), new GcStatz(gcBean, gcBean.getName()));
      }
      MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
      this.heap = new HeapStatz(memoryBean.getHeapMemoryUsage(), "heap");
      this.nonHeap = new HeapStatz(memoryBean.getNonHeapMemoryUsage(), "non_heap");
      this.os = new OsStatz();
    }
    
    public void write(PrintWriter writer) {
       for (Map.Entry<String, GcStatz> entry : this.gc.entrySet()) {
         String key = entry.getKey();
         GcStatz value = entry.getValue();

         value.write(writer);
       }

       this.heap.write(writer);
       this.nonHeap.write(writer);
       this.os.write(writer);
    }


  }
   static class GcStatz {
    long collectionCount;
    long collectionTime;
    String description;
     GcStatz(GarbageCollectorMXBean gcBean, String desc) {
      this.collectionCount = gcBean.getCollectionCount();
      this.collectionTime = gcBean.getCollectionTime();
      this.description = desc;
    }
    public void write(PrintWriter writer) {
      String name = this.description.replaceAll(" ", "");
      String cname = "collection_count_" + name;
      String ctime = "collection_time_" + name;
      writePromDoc(writer, cname, "", "count");
       writer.printf("%s %d", cname, this.collectionCount);
       writer.println();
      writePromDoc(writer, ctime, "", "timer");
       writer.printf("%s %d", ctime, this.collectionTime);
       writer.println();
    }
  }
   static class HeapStatz {
    long committed;
    long used;
    String description;
     HeapStatz(MemoryUsage memoryUsage, String desc) {
      this.committed = memoryUsage.getCommitted();
      this.used = memoryUsage.getUsed();
      this.description = desc;
    }
    public void write(PrintWriter writer) {
      String cmem_name = "committed_memory_" + this.description + "_count";
      String umem_name = "used_memory_" + this.description + "_count";

      writePromDoc(writer, cmem_name, "", "count" );
           writer.printf("%s %d", cmem_name, this.committed);
       writer.println();
      writePromDoc(writer, umem_name, "", "count" );
       writer.printf("%s %d", umem_name, this.used);
       writer.println();
    }
  }
   static class OsStatz {
    long openFileDescriptorCount;
     OsStatz() {
      UnixOperatingSystemMXBean osBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      this.openFileDescriptorCount = osBean.getOpenFileDescriptorCount();
    }
    public void write(PrintWriter writer) {
       writePromDoc(writer, "open file descriptors", "", "count");
      writer.printf("open_file_descriptor_count %d", this.openFileDescriptorCount);
      writer.println();
    }

  }
}

