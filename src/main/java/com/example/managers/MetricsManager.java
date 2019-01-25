package com.example.managers;

import com.codahale.metrics.*;
import io.vertx.core.logging.Logger;
import io.vertx.ext.dropwizard.MetricsService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;

/**
 * metric manager.
 */
public class MetricsManager {

    /**
     * logger.
     */
    private Logger logger = null;
    /**
     * metric base name.
     */
    private String metricBaseName = null;
    /**
     * registry.
     */
    private MetricRegistry registry = null;
    /**
     * metric service.
     */
    private MetricsService metricsService = null;
    /**
     * hash map for guage integer.
     */
    private ConcurrentHashMap<String, Gauge<Integer>> gaugeIntegerConcurrentHashMap = null;
    /**
     * hash map for gauge double.
     */
    private ConcurrentHashMap<String, Gauge<Double>> gaugeDoubleConcurrentHashMap = null;
    /**
     * hash map for counter.
     */
    private ConcurrentHashMap<String, Counter> counterConcurrentHashMap = null;
    /**
     * hash map for histogram.
     */
    private ConcurrentHashMap<String, Histogram> histogramConcurrentHashMap = null;
    /**
     * hash map for meter.
     */
    private ConcurrentHashMap<String, Meter> meterConcurrentHashMap = null;
    /**
     * hash map for timer.
     */
    private ConcurrentHashMap<String, Timer> timerConcurrentHashMap = null;

    /**
     * init.
     *
     * @param logger1         logger
     * @param metricBaseName1 name
     */
    public MetricsManager(final Logger logger1, final String metricBaseName1) {
        this.registry = SharedMetricRegistries.getOrCreate("gis-registry");
        this.logger = logger1;
        this.metricBaseName = metricBaseName1;
    }

    /**
     * get registry.
     *
     * @return registry
     */
    public MetricRegistry getMetricRegistry() {
        return registry;
    }

    /**
     * get gauge.
     *
     * @param gaugeName name
     * @return gauge
     */
    public Gauge getGauge(final String gaugeName) {
        return registry.getGauges().get(metricBaseName + ".guage." + gaugeName);
    }

    /**
     * get counter.
     *
     * @param counterName name
     * @return counter
     */
    public Counter getCounter(final String counterName) {
        Counter counter = null;
        try {
            counter = counterConcurrentHashMap.get(counterName);
            if (counter == null) {
                counter = registry.counter(MetricRegistry.name(metricBaseName + ".counter", counterName));
                counterConcurrentHashMap.put(counterName, counter);
            }
        } catch (NullPointerException e) {
            counterConcurrentHashMap = new ConcurrentHashMap<String, Counter>();
            counter = registry.counter(MetricRegistry.name(metricBaseName + ".counter", counterName));
            counterConcurrentHashMap.put(counterName, counter);
        } catch (Exception e) {
            printStackTrace(e, logger);
        }
        return counter;
    }

    /**
     * get histogram.
     *
     * @param histogramName name
     * @return histogram
     */
    public Histogram getHistogram(final String histogramName) {
        Histogram histogram = null;
        try {
            histogram = histogramConcurrentHashMap.get(histogramName);
            if (histogram == null) {
                histogram = registry.histogram(MetricRegistry.name(metricBaseName + ".histogram", histogramName));
                histogramConcurrentHashMap.put(histogramName, histogram);
            }
        } catch (NullPointerException e) {
            histogramConcurrentHashMap = new ConcurrentHashMap<String, Histogram>();
            histogram = registry.histogram(MetricRegistry.name(metricBaseName + ".histogram", histogramName));
            histogramConcurrentHashMap.put(histogramName, histogram);
        } catch (Exception e) {
            printStackTrace(e, logger);
        }
        return histogram;
    }

    /**
     * get meter.
     *
     * @param meterName name
     * @return meter
     */
    public Meter getMeter(final String meterName) {
        Meter meter = null;
        try {
            meter = meterConcurrentHashMap.get(meterName);
            if (meter == null) {
                String name = MetricRegistry.name(metricBaseName + ".meter", meterName);
                meter = registry.meter(MetricRegistry.name(name));
                meterConcurrentHashMap.put(meterName, meter);
            }
        } catch (NullPointerException e) {
            meterConcurrentHashMap = new ConcurrentHashMap<String, Meter>();
            String name = MetricRegistry.name(metricBaseName + ".meter", meterName);
            meter = registry.meter(MetricRegistry.name(name));
            meterConcurrentHashMap.put(meterName, meter);
        } catch (Exception e) {
            printStackTrace(e, logger);
        }
        return meter;
    }

    /**
     * get timer.
     *
     * @param timerName name
     * @return timer
     */
    public Timer getTimer(final String timerName) {
        Timer timer = null;
        try {
            timer = timerConcurrentHashMap.get(timerName);
            if (timer == null) {
                String name = MetricRegistry.name(metricBaseName + ".timer", timerName);
                timer = registry.timer(MetricRegistry.name(name));
                timerConcurrentHashMap.put(timerName, timer);
            }
        } catch (NullPointerException e) {
            timerConcurrentHashMap = new ConcurrentHashMap<String, Timer>();
            timer = registry.timer(MetricRegistry.name(metricBaseName + ".timer", timerName));
            timerConcurrentHashMap.put(timerName, timer);
        } catch (Exception e) {
            printStackTrace(e, logger);
        }
        return timer;
    }

    /**
     * get metric log.
     *
     * @return log
     */
    public String getMetricLog() {
        StringBuilder metricsLog = new StringBuilder();
        for (final String key : registry.getCounters().keySet()) {
            Counter counter = registry.getCounters().get(key);
            if (counter != null) {
                metricsLog.append(key + ":" + counter.getCount()).append("\n\r");
            }
        }
        for (final String key : registry.getHistograms().keySet()) {
            long max = registry.getHistograms().get(key).getSnapshot().getMax();
            Snapshot sh = registry.getHistograms().get(key).getSnapshot();
            metricsLog
                    .append(key
                            + (max > 1000000
                            ? (max > 10000000
                            ? (max > 100000000
                            ? (max > 1000000000
                            ? (max > 10000000000l ? "\t10----->" : "\t9---->")
                            : "\t8--->")
                            : "\t7-->")
                            : "\t6->")
                            : "\t")
                            + "MAX:" + max)
                    .append("\tMEAN:" + sh.getMean()).append("\tMIN:" + sh.getMin())
                    .append("\t75%:" + sh.get75thPercentile()).append("\t95%:" + sh.get95thPercentile())
                    .append("\t98%:" + sh.get98thPercentile()).append("\t99%:" + sh.get99thPercentile())
                    .append("\t999%:" + sh.get999thPercentile()).append("\tStdDev:" + sh.getStdDev()).append("\n\r");
        }
        for (final String key : registry.getMeters().keySet()) {
            Meter mt = registry.getMeters().get(key);
            metricsLog.append(key + "\tCount:" + mt.getCount() + "\tMeanRate:" + mt.getMeanRate())
                    .append("\tOneMinuteRate:" + mt.getOneMinuteRate())
                    .append("\tFiveOneMinuteRate:" + mt.getFiveMinuteRate())
                    .append("\tFifteenMinuteRate:" + mt.getFifteenMinuteRate()).append("\n\r");
        }
        for (final String key : registry.getGauges().keySet()) {
            metricsLog.append(key + ":" + registry.getGauges().get(key).getValue()).append("\n\r");
        }
        final String metricsLogString = metricsLog.toString();
        return metricsLogString;
    }

    /**
     * print statcktrace.
     *
     * @param e      exception
     * @param logger logger
     */
    public static void printStackTrace(final Exception e, final Logger logger) {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        logger.error(errors.toString());
    }
}
