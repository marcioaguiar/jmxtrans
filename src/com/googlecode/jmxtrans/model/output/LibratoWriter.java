/*
 * Copyright (c) 2010-2013 the original author or authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 */
package com.googlecode.jmxtrans.model.output;


import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.util.BaseOutputWriter;
import com.googlecode.jmxtrans.util.JmxUtils;
import com.googlecode.jmxtrans.util.ValidationException;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.Base64Variants;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <a href="https://metrics.librato.com//">Librato Metrics</a> implementation of the {@linkplain org.jmxtrans.embedded.output.OutputWriter}.
 * <p/>
 * This implementation uses <a href="http://dev.librato.com/v1/post/metrics">
 * POST {@code /v1/metrics}</a> HTTP API.
 * <p/>
 * {@link LibratoWriter} uses the "{@code query.attribute.type}" configuration parameter (via
 * {@link org.jmxtrans.embedded.QueryResult#getType()}) to publish the metrics.<br/>
 * Supported types are {@value #METRIC_TYPE_COUNTER} and {@value #METRIC_TYPE_GAUGE}.<br/>
 * If the type is <code>null</code> or unsupported, metric is exported
 * as {@value #METRIC_TYPE_COUNTER}.
 * <p/>
 * Settings:
 * <ul>
 * <li>"{@code url}": Librato server URL.
 * Optional, default value: {@value #DEFAULT_LIBRATO_API_URL}.</li>
 * <li>"{@code user}": Librato user. Mandatory</li>
 * <li>"{@code token}": Librato token. Mandatory</li>
 * <li>"{@code libratoApiTimeoutInMillis}": read timeout of the calls to Librato HTTP API.
 * Optional, default value: {@value #DEFAULT_LIBRATO_API_TIMEOUT_IN_MILLIS}.</li>
 * <li>"{@code enabled}": flag to enable/disable the writer. Optional, default value: <code>true</code>.</li>
 * <li>"{@code source}": Librato . Optional, default value: {@value #DEFAULT_SOURCE} (the hostname of the server).</li>
 * </ul>
 *
 * @author <a href="mailto:cleclerc@cloudbees.com">Cyrille Le Clerc</a>
 */
public class LibratoWriter extends BaseOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(LibratoWriter.class);
    
    public final static String SETTING_URL = "url";
    public final static String SETTING_USERNAME = "username";
    public final static String SETTING_TOKEN = "token";
    public final static String SETTING_PROXY_PORT = "proxyPort";
    public final static String SETTING_PROXY_HOST = "proxyHost";
    public final static String SETTING_NAME_PREFIX = "namePrefix";
    
    public static final String METRIC_TYPE_GAUGE = "gauge";
    public static final String METRIC_TYPE_COUNTER = "counter";
    public static final String DEFAULT_LIBRATO_API_URL = "https://metrics-api.librato.com/v1/metrics";
    public static final String SETTING_LIBRATO_API_TIMEOUT_IN_MILLIS = "libratoApiTimeoutInMillis";
    public static final int DEFAULT_LIBRATO_API_TIMEOUT_IN_MILLIS = 1000;
    public static final String SETTING_SOURCE = "source";
    public static final String DEFAULT_SOURCE = "#hostname#";
    
    private final AtomicInteger exceptionCounter = new AtomicInteger();
    /**
     * Librato HTTP API URL
     */
    private URL url;
    private int libratoApiTimeoutInMillis = DEFAULT_LIBRATO_API_TIMEOUT_IN_MILLIS;
    /**
     * Librato HTTP API authentication username
     */
    private String user;
    /**
     * Librato HTTP API authentication token
     */
    private String token;
    private String basicAuthentication;
    /**
     * Optional proxy for the http API calls
     */
    private Proxy proxy;
    /**
     * Librato measurement property 'source',
     */
    private String source;

    /**
     * Load settings<p/>
     */
    public void start(Map<String, String> settings) {
    }
    
    /**
     * Send given metrics to the Graphite server.
     */
    public void write(List<Result> results) {
        log.debug("Export to librato");

        List<Result> counters = new ArrayList<Result>();
        List<Result> gauges = new ArrayList<Result>();
        for (Result result : results) {
//            if (METRIC_TYPE_GAUGE.equals(result.getType())) {
//                gauges.add(result);
//            } else if (METRIC_TYPE_COUNTER.equals(result.getType())) {
//                counters.add(result);
//            } else if (null == result.getType()) {
//                log.log(getInfoLevel(), "Unspecified type for result {}, export it as counter");
//                counters.add(result);
//            } else {
//                logger.log(getInfoLevel(), "Unsupported metric type '" + result.getType() + "' for result " + result + ", export it as counter");
//                counters.add(result);
//            }
            if (StringUtils.contains(result.getClassNameAlias(), ".counter.") 
                    || "Count".equals(result.getAttributeName())) {
                counters.add(result);
            } else {
                gauges.add(result);
            }
            
        }
        
        HttpURLConnection urlConnection = null;
        try {
            if (proxy == null) {
                urlConnection = (HttpURLConnection) url.openConnection();
            } else {
                urlConnection = (HttpURLConnection) url.openConnection(proxy);
            }
            urlConnection.setRequestMethod("POST");
            urlConnection.setDoInput(true);
            urlConnection.setDoOutput(true);
            urlConnection.setReadTimeout(libratoApiTimeoutInMillis);
            urlConnection.setRequestProperty("content-type", "application/json; charset=utf-8");
            urlConnection.setRequestProperty("Authorization", "Basic " + basicAuthentication);

            serialize(counters, gauges, urlConnection.getOutputStream());
            int responseCode = urlConnection.getResponseCode();
            if (responseCode != 200) {
                exceptionCounter.incrementAndGet();
                log.warn("Failure {}:'{}' to send result to Librato server '{}' with proxy {}, user {}", 
                        responseCode, urlConnection.getResponseMessage(), url, proxy, user);
            }
        } catch (Exception e) {
            exceptionCounter.incrementAndGet();
            log.warn("Failure to send result to Librato server '{}' with proxy {}, user {}", url, proxy, user);
            log.warn("Caused by", e);
        } finally {
            if (urlConnection != null) {
                try {
                    InputStream in = urlConnection.getInputStream();
                    copy(in, nullOutputStream());
                    closeQuietly(in);
                    InputStream err = urlConnection.getErrorStream();
                    if (err != null) {
                        copy(err, nullOutputStream());
                        closeQuietly(err);
                    }
                } catch (IOException e) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    try {
                        serialize(counters, gauges, out);
                    } catch (IOException ex) {
                        log.warn("dammasdasmdasd");
                    }
                    log.warn("Exception flushing http connection", e);
                    log.warn(out.toString());
                }
            }

        }
    }

    public void serialize(Iterable<Result> counters, Iterable<Result> gauges, OutputStream out) throws IOException {
        
        LibratoMetricSet metricSet = new LibratoMetricSet();
        
        for (Result counter : counters) {
            metricSet.addCounter(new LibratoMetric(counter));
        }
        
        for (Result gauge : gauges) {
            metricSet.addGauge(new LibratoMetric(gauge));
        }
        
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibilityChecker(
            mapper.getVisibilityChecker().withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        );
        mapper.writeValue(out, metricSet);
    }

    public int getExceptionCounter() {
        return exceptionCounter.get();
    }
    
    private static OutputStream nullOutputStream() {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
            }
        };
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[512];
        int len;
        while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
        }
    }

    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            log.debug("Exception closing quietly", e);
        }
    }

    public void doWrite(Query query) throws Exception {
        write(query.getResults());
    }

    public void validateSetup(Query query) throws ValidationException {
        try {
            url = new URL(this.getStringSetting(SETTING_URL, DEFAULT_LIBRATO_API_URL));

            user = this.getStringSetting(SETTING_USERNAME, null);
            token = this.getStringSetting(SETTING_TOKEN, null);
            basicAuthentication = Base64Variants.getDefaultVariant().encode((user + ":" + token).getBytes(Charset.forName("US-ASCII")));

            if (this.getStringSetting(SETTING_PROXY_HOST, null) != null && !this.getStringSetting(SETTING_PROXY_HOST, "").isEmpty()) {
                proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(this.getStringSetting(SETTING_PROXY_HOST, null), this.getIntegerSetting(SETTING_PROXY_PORT, null)));
            }

            libratoApiTimeoutInMillis = this.getIntegerSetting(SETTING_LIBRATO_API_TIMEOUT_IN_MILLIS, DEFAULT_LIBRATO_API_TIMEOUT_IN_MILLIS);

            source = this.getStringSetting(SETTING_SOURCE, DEFAULT_SOURCE);
            log.info("Start Librato writer connected to '{}'", url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public class LibratoMetric {
        
        private final String name;
        private final long measure_time;
        private float value;
        private final String source;
        
        public LibratoMetric(Result result) {
            this.name = result.getClassNameAlias() + "." + result.getAttributeName();
            this.measure_time = TimeUnit.SECONDS.convert(result.getEpoch(), TimeUnit.MILLISECONDS);
            this.source = LibratoWriter.this.source;
            
            Map<String, Object> resultValues = result.getValues();
            
            if (resultValues != null) {
                for (Map.Entry<String, Object> values : resultValues.entrySet()) {
                    Object val = values.getValue();

                    if (JmxUtils.isNumeric(val)) {
                        this.value = Float.parseFloat(val.toString());
                    } else {
                        log.warn("Unable to submit non-numeric value to Graphite: '{}' from result '{}'",val, result);
                    }
                }
            }
        }
    }
    
    public class LibratoMetricSet {
        
        private List<LibratoMetric> counters = new ArrayList<LibratoWriter.LibratoMetric>();
        private List<LibratoMetric> gauges = new ArrayList<LibratoWriter.LibratoMetric>();
        
        public void addCounter(LibratoMetric metric) {
            this.counters.add(metric);
        }
        
        public void addGauge(LibratoMetric metric) {
            this.gauges.add(metric);
        }
    }
}
