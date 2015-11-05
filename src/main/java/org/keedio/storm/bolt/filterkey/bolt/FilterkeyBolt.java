package org.keedio.storm.bolt.filterkey.bolt;

import info.ganglia.gmetric4j.gmetric.GMetric;
import org.keedio.storm.bolt.filterkey.metrics.MetricsController;
import org.keedio.storm.bolt.filterkey.metrics.MetricsEvent;
import org.keedio.storm.bolt.filterkey.metrics.SimpleMetric;
import org.keedio.storm.bolt.filterkey.services.Filtering;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Luis Lázaro lalazaro@keedio.com Keedio
 */
public class FilterkeyBolt implements IRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterkeyBolt.class);

    private Filtering filtering;
    private OutputCollector collector;
    private MetricsController mc;
    private int refreshTime;
    private Date lastExecution = new Date();

    //for Ganglia only
    private String hostGanglia, reportGanglia;
    private GMetric.UDPAddressingMode modeGanglia;
    private int portGanglia, ttlGanglia;
    private int minutesGanglia;

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        filtering = new Filtering();
        filtering.setProperties(config);

        if (config.get("refreshtime") == null)
            refreshTime = 10;
        else
            refreshTime = Integer.parseInt((String) config.get("refreshtime"));

        //check if in topology's config ganglia.report is set to "yes"
        if (loadGangliaProperties(config)) {
            mc = new MetricsController(hostGanglia, portGanglia, modeGanglia, ttlGanglia, minutesGanglia);
        } else {
            mc = new MetricsController();
        }

        mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "filtered"));
        mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "unfiltered"));
        mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "errorFilter"));

        // Registramos la metrica para su publicacion
        SimpleMetric filtered = new SimpleMetric(mc.getMetrics(), "filtered", SimpleMetric.TYPE_METER);
        SimpleMetric unfiltered = new SimpleMetric(mc.getMetrics(), "unfiltered", SimpleMetric.TYPE_METER);
        SimpleMetric errorFilter = new SimpleMetric(mc.getMetrics(), "errorFilter", SimpleMetric.TYPE_METER);
        SimpleMetric histogram = new SimpleMetric(mc.getMetrics(), "histogram", SimpleMetric.TYPE_HISTOGRAM);

        context.registerMetric("filtered", filtered, refreshTime);
        context.registerMetric("unfiltered", unfiltered, refreshTime);
        context.registerMetric("errorFilter", errorFilter, refreshTime);
        context.registerMetric("histogram", histogram, refreshTime);
    }

    @Override
    public void execute(Tuple tuple) {
        // Añadimos al throughput e inicializamos el date
        Date actualDate = new Date();
        long aux = (actualDate.getTime() - lastExecution.getTime()) / 1000;
        lastExecution = actualDate;

        // Registramos para calculo de throughput
        mc.manage(new MetricsEvent(MetricsEvent.UPDATE_THROUGHPUT, aux));

        String event = tuple.getString(0);
        try {
            Map<String, String> map = extractExtradata(event);
            Map<String, String> mapFiltered = filtering.filterMap(map);

            String message = extractMessage(event);
            JSONObject extradataObject = new JSONObject();

            if (mapFiltered.isEmpty()) {
                extradataObject = this.toJsonExtradata(map);
                mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "unfiltered"));
            } else {
                extradataObject = this.toJsonExtradata(mapFiltered);
                mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "filtered"));
            }

            JSONObject messageObject = this.toJsonMessage(message);
            JSONObject mainObject = new JSONObject();

            mainObject.putAll(extradataObject);
            mainObject.putAll(messageObject);

            LOGGER.info("Will emit: " + mainObject + " \n");
            collector.emit(new Values(mainObject.toJSONString()));
            collector.ack(tuple);

        } catch (ParseException e) {
            LOGGER.error("", e);
            collector.fail(tuple);
            mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "errorFilter"));
        }
    }

    /**
     * Extract named field "extraData" and return a map of its
     * values.
     *
     * @param event of tuple
     * @return map extracted
     * @throws ParseException exception
     */
    public Map<String, String> extractExtradata(String event) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(event);
        JSONObject obj2 = (JSONObject) obj.get("extraData");
        Map<String, String> mapOfExtradata = new HashMap<>();
        mapOfExtradata = (Map) obj2;
        return mapOfExtradata;
    }

    /**
     * extract named field "message" and return body without changes
     *
     * @param event string
     * @return string
     * @throws ParseException parseException
     */
    public String extractMessage(String event) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(event);
        String originalMessage = (String) obj.get("message");
        return originalMessage;
    }

    /**
     * Make a Json with a property called extraData and a value
     * cotaining a map.
     *
     * @param map of data
     * @return jsonobject
     */
    public JSONObject toJsonExtradata(Map<String, String> map) {

        JSONObject json1 = new JSONObject();
        json1.putAll(map);

        JSONObject json2 = new JSONObject();
        json2.putAll(json1);

        JSONObject mainObj = new JSONObject();
        mainObj.put("extraData", json2);

        return mainObj;
    }

    /**
     * Make a Json with a property called "message" and a value
     * containig a String
     *
     * @param mes message
     * @return Jsonobject
     */
    public JSONObject toJsonMessage(String mes) {

        JSONObject mainObj = new JSONObject();
        mainObj.put("message", mes);

        return mainObj;
    }


    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * ganglia's server properties are taken from main topology's config
     *
     * @param stormConf
     * @return
     */
    public boolean loadGangliaProperties(Map stormConf) {
        boolean loaded = false;
        if (stormConf.containsKey("ganglia.report")) {
            reportGanglia = (String) stormConf.get("ganglia.report");
            if (reportGanglia.equals("yes")) {
                hostGanglia = (String) stormConf.get("ganglia.host");
                portGanglia = Integer.parseInt((String) stormConf.getOrDefault("ganglia.port", "5555"));
                ttlGanglia = Integer.parseInt((String) stormConf.getOrDefault("ganglia.ttl", "1"));
                minutesGanglia = Integer.parseInt((String) stormConf.getOrDefault("ganglia.minutes", "1"));
                String stringModeGanglia = (String) stormConf.getOrDefault("ganglia.UDPAddressingMode", "MULTICAST");
                modeGanglia = GMetric.UDPAddressingMode.valueOf(stringModeGanglia);
                loaded = true;
            }
        } else {
            stormConf.put("ganglia.report", "no");
        }
        return loaded;
    }

}