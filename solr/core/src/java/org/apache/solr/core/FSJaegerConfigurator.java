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

package org.apache.solr.core;

import javax.servlet.http.HttpServletRequest;
import java.lang.invoke.MethodHandles;

import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.reporters.LoggingReporter;
import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.solr.common.util.NamedList;
import io.jaegertracing.internal.samplers.ConstSampler;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.solr.util.tracing.HttpServletCarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class FSJaegerConfigurator extends TracerConfigurator implements GlobalTracer.SolrTracer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final ThreadLocal<Boolean> IS_FORWARDED = ThreadLocal.withInitial(() -> Boolean.FALSE);

  private volatile Tracer tracer;
  @Override
  public Tracer getTracer() {
    return tracer;
  }

  @Override
  public boolean enabled() {
    return MDC.get(_REQ_ID) != null;
  }


  public void requestStart(HttpServletRequest request) {
    String reqId = MDC.get(_REQ_ID);
    if (reqId == null) return;
    if (log.isInfoEnabled()) {
      String type = IS_FORWARDED.get() ? "forwarded" : "external";
      log.info("{} request, REQ_ID: {} , for path:{} {} ", type, reqId,request.getMethod(), request.getRequestURI());
    }
  }

  @Override
  public void init(NamedList args) {
    JaegerTracer.Builder builder = new JaegerTracer.Builder("solr")
        .withSampler(new ConstSampler(true))
        .withReporter(new LoggingReporter(log))

       ;
    tracer= new TracerWrapper(builder.build());
  }

  public class TracerWrapper implements Tracer {
    private final  Tracer delegate;

    public TracerWrapper(Tracer delegate) {
      this.delegate = delegate;
    }

    @Override
    public ScopeManager scopeManager() {
      return delegate.scopeManager();
    }

    @Override
    public Span activeSpan() {
      return delegate.activeSpan();
    }

    @Override
    public Scope activateSpan(Span span) {
      return delegate.activateSpan(span);
    }

    @Override
    public SpanBuilder buildSpan(String s) {
      return delegate.buildSpan(s);
    }

    @Override
    public <C> void inject(SpanContext spanContext, Format<C> format, C c) {
      if(MDC.get(_REQ_ID) == null) return;
      String reqId = MDC.get(_REQ_ID);
      if(reqId != null) {
        spanContext =  ((JaegerSpanContext) spanContext).withBaggageItem(_REQ_ID, reqId);
      }
      delegate.inject(spanContext, format, c);
    }

    @Override
    public <C> SpanContext extract(Format<C> format, C c) {
      IS_FORWARDED.remove();
      JaegerSpanContext result;
      try {
        result = (JaegerSpanContext) delegate.extract(format, c);
        if(result == null) return result;
        String fsId = result.getBaggageItem(_REQ_ID);
        if(fsId != null) {
          MDC.put(_REQ_ID, fsId);
          IS_FORWARDED.set(Boolean.TRUE);
        }
      } finally {
        if (c instanceof HttpServletCarrier) {
          HttpServletCarrier entries = (HttpServletCarrier) c;
          requestStart(entries.getRequest());
        }

      }
      return result;
    }

    @Override
    public void close() {
      delegate.close();

    }

  }
  public static final String _REQ_ID = "_REQ_ID";

}
