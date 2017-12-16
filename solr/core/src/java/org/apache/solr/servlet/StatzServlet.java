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
import org.noggit.JSONUtil;
import org.noggit.JSONWriter;

/**
 * FullStory: a simple servlet to produce a few JSON stats.
 */
public final class StatzServlet extends BaseSolrServlet {

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
    pw.println(JSONUtil.toJSON(statz));
    pw.flush();
  }

  static class Statz implements JSONWriter.Writable {
    Map<String, GcStatz> gc;
    HeapStatz heap;
    HeapStatz nonHeap;
    OsStatz os;

    Statz() {
      this.gc = new LinkedHashMap<>();
      for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
        this.gc.put(gcBean.getName(), new GcStatz(gcBean));
      }

      MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
      this.heap = new HeapStatz(memoryBean.getHeapMemoryUsage());
      this.nonHeap = new HeapStatz(memoryBean.getNonHeapMemoryUsage());
      this.os = new OsStatz();
    }

    @Override
    public void write(JSONWriter writer) {
      writer.startObject();
      writer.indent();

      writer.writeString("gc");
      writer.writeNameSeparator();
      writer.write(this.gc);

      writer.writeValueSeparator();
      writer.indent();

      writer.writeString("heap");
      writer.writeNameSeparator();
      writer.write(this.heap);

      writer.writeValueSeparator();
      writer.indent();

      writer.writeString("nonheap");
      writer.writeNameSeparator();
      writer.write(this.nonHeap);

      writer.writeValueSeparator();
      writer.indent();

      writer.writeString("os");
      writer.writeNameSeparator();
      writer.write(this.os);

      writer.endObject();
    }
  }

  static class GcStatz implements JSONWriter.Writable {
    long collectionCount;
    long collectionTime;

    GcStatz(GarbageCollectorMXBean gcBean) {
      this.collectionCount = gcBean.getCollectionCount();
      this.collectionTime = gcBean.getCollectionTime();
    }

    @Override
    public void write(JSONWriter writer) {
      writer.startObject();
      writer.indent();

      writer.writeString("collectionCount");
      writer.writeNameSeparator();
      writer.write(this.collectionCount);

      writer.writeValueSeparator();
      writer.indent();

      writer.writeString("collectionTime");
      writer.writeNameSeparator();
      writer.write(this.collectionTime);

      writer.endObject();
    }
  }

  static class HeapStatz implements JSONWriter.Writable {
    long committed;
    long used;

    HeapStatz(MemoryUsage memoryUsage) {
      this.committed = memoryUsage.getCommitted();
      this.used = memoryUsage.getUsed();
    }

    @Override
    public void write(JSONWriter writer) {
      writer.startObject();
      writer.indent();

      writer.writeString("committed");
      writer.writeNameSeparator();
      writer.write(this.committed);

      writer.writeValueSeparator();
      writer.indent();

      writer.writeString("used");
      writer.writeNameSeparator();
      writer.write(this.used);

      writer.endObject();
    }
  }

  static class OsStatz implements JSONWriter.Writable {
    long openFileDescriptorCount;

    OsStatz() {
      UnixOperatingSystemMXBean osBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      this.openFileDescriptorCount = osBean.getOpenFileDescriptorCount();
    }

    @Override
    public void write(JSONWriter writer) {
      writer.startObject();
      writer.indent();

      writer.writeString("openFileDescriptorCount");
      writer.writeNameSeparator();
      writer.write(this.openFileDescriptorCount);

      writer.endObject();
    }
  }
}
