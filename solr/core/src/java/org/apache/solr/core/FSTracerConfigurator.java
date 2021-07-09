package org.apache.solr.core;

import javax.servlet.http.HttpServletRequest;
import java.lang.invoke.MethodHandles;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.reporters.LoggingReporter;
import io.jaegertracing.internal.reporters.NoopReporter;
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

public class FSTracerConfigurator extends TracerConfigurator  {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final ThreadLocal<Boolean> IS_FORWARDED = ThreadLocal.withInitial(() -> Boolean.FALSE);

  private volatile Tracer tracer;
  @Override
  public Tracer getTracer() {
    return tracer;
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
        .withReporter(new NoopReporter())
       ;
    tracer= new TracerWrapper(builder.build());
  }

  public class TracerWrapper implements Tracer , GlobalTracer.SolrTracer {
    private final  Tracer delegate;

    @Override
    public boolean enabled() {
      return MDC.get(_REQ_ID) != null;
    }
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
      String reqId = MDC.get(_REQ_ID);
      if(reqId == null) return;
      spanContext =  ((JaegerSpanContext) spanContext).withBaggageItem(_REQ_ID, reqId);
      delegate.inject(spanContext, format, c);
    }

    @Override
    public <C> SpanContext extract(Format<C> format, C c) {
      IS_FORWARDED.remove();
      JaegerSpanContext result;
      try {
        result = (JaegerSpanContext) delegate.extract(format, c);
        if(result == null) return result;
        String reqId = result.getBaggageItem(_REQ_ID);
        if(reqId != null) {
          MDC.put(_REQ_ID, reqId);
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
  public static final String _REQ_ID = "_req_id";

}
