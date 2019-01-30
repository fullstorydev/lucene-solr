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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import sun.misc.Signal;

/**
 * FullStory: a servlet to remotely kill Solr.
 *
 * <p>PUT: raises SIGTERM. Tasks will be scheduled (on a daemon thread, so as not
 * to delay JVM termination) to call {@code System.exit(1)} after 60 seconds and
 * then {@code Runtime.halt(1)} after 90 seconds, to make sure that the JVM does
 * eventually terminate, regardless of the state of non-daemon threads and
 * shutdown handlers.
 */
public final class QuitQuitQuitServlet extends BaseSolrServlet {

  private static final ScheduledExecutorService sched =
      Executors.newScheduledThreadPool(1, r -> {
        Thread th = new Thread(r);
        th.setDaemon(true);
        return th;
      });

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setStatus(HttpServletResponse.SC_OK);
    resp.setContentLength(2);
    PrintWriter w = resp.getWriter();
    w.write("OK");
    w.flush();

    sched.schedule(() -> System.exit(1), 60, TimeUnit.SECONDS);
    sched.schedule(() -> Runtime.getRuntime().halt(1), 90, TimeUnit.SECONDS);
    sched.execute(() -> Signal.raise(new Signal("TERM")));
  }
}
