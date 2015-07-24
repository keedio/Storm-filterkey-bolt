package org.keedio.storm.bolt.filterkey.metrics;

/**
 * Created by luislazaro on 24/7/15.
 * lalazaro@keedio.com
 * Keedio
 */
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import backtype.storm.metric.api.IMetric;

public class SimpleMetric implements IMetric{

    public static final int TYPE_METER = 1;
    public static final int TYPE_HISTOGRAM = 2;

    private MetricRegistry metric;
    private String metricId;
    private int type;

    public SimpleMetric(MetricRegistry metric, String metricId, int type) {
        // TODO Auto-generated constructor stub
        this.metric = metric;
        this.metricId = metricId;
        this.type = type;
    }

    @Override
    public Object getValueAndReset() {
        Map<String, Object> map = new HashMap<String, Object>();

        switch (type) {
            case TYPE_METER:
                Meter meter = metric.meter(metricId);

                map.put("count", meter.getCount());
                map.put("fifteenminuterate", meter.getFifteenMinuteRate());
                map.put("fiveminuterate", meter.getFiveMinuteRate());
                map.put("meanrate", meter.getMeanRate());
                map.put("oneminuterate", meter.getOneMinuteRate());


                break;
            case TYPE_HISTOGRAM:
                Histogram histogram = metric.histogram(metricId);

                map.put("count", histogram.getCount());
                map.put("75thpercentile", histogram.getSnapshot().get75thPercentile());
                map.put("95thpercentile", histogram.getSnapshot().get95thPercentile());
                map.put("98thpercentile", histogram.getSnapshot().get98thPercentile());
                map.put("999thpercentile", histogram.getSnapshot().get999thPercentile());
                map.put("99thpercentile", histogram.getSnapshot().get99thPercentile());
                map.put("max", histogram.getSnapshot().getMax());
                map.put("mean", histogram.getSnapshot().getMean());
                map.put("median", histogram.getSnapshot().getMedian());
                map.put("min", histogram.getSnapshot().getMin());
                map.put("stddev", histogram.getSnapshot().getStdDev());

                break;
        }

        return map;
    }

}
