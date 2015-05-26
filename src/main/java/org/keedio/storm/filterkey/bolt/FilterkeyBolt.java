
package org.keedio.storm.filterkey.bolt;

import org.keedio.storm.filterkey.services.Filtering;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

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

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        filtering = new Filtering();
        filtering.setProperties(config);
    }

    @Override
    public void execute(Tuple tuple) {
        String event = tuple.getString(0);
        try {
            Map<String, String> map = extractExtradata(event);
            Map<String, String> mapFiltered = filtering.filterMap(map);
            String message = extractMessage(event);
            collector.emit(tuple, new Values(mapFiltered, message));
            collector.ack(tuple);
        } catch (ParseException e) {
            collector.fail(tuple);
            LOGGER.error("", e);
        }
    }

    /**
     * Extract named field "extraData" and return a map of its
     * values.
     *
     * @param event
     * @return
     * @throws ParseException
     *
     */
    public Map<String, String> extractExtradata(String event) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(event);
        JSONObject obj2 = (JSONObject) obj.get("extraData");
        Map<String, String> mapOfExtradata = new HashMap<>();
        mapOfExtradata = (Map) obj2.get("extraData");
        return mapOfExtradata;
    }

    /**
     * extract named field "message" and return body without changes
     * @param event
     * @return
     * @throws ParseException
     */
    public String extractMessage(String event) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(event);
        JSONObject obj2 = (JSONObject) obj.get("message");
        String originalMessage = (String) obj2.get("message");
        return originalMessage;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("extraData", "message"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
