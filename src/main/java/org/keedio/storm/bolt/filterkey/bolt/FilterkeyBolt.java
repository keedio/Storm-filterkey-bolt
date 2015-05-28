
package org.keedio.storm.bolt.filterkey.bolt;

import org.keedio.storm.bolt.filterkey.services.Filtering;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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

            JSONObject extradataObject = this.toJsonExtradata(mapFiltered);
            JSONObject messageObject = this.toJsonMessage(message);
            JSONObject mainObject = new JSONObject();

            mainObject.putAll(extradataObject);
            mainObject.putAll(messageObject);

            System.out.println("will emit: " + mainObject + " \n");
            collector.emit(new Values(mainObject.toJSONString()));
            collector.ack(tuple);

        } catch (ParseException e) {
            LOGGER.error("", e);
            collector.fail(tuple);
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
     * @param map of data
     * @return jsonobject
     */
    public JSONObject toJsonExtradata(Map<String, String> map){

        JSONObject json1 = new JSONObject();
        json1.putAll(map);

        JSONObject json2 = new JSONObject();
        json2.putAll(json1);

        JSONObject mainObj = new JSONObject();
        mainObj.put("extraData", json2);

        return  mainObj;
    }

    /**
     * Make a Json with a property called "message" and a value
     * containig a String
     * @param mes message
     * @return Jsonobject
     */
    public JSONObject toJsonMessage(String mes){

        JSONObject mainObj = new JSONObject();
        mainObj.put("message", mes);

        return  mainObj;
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

}
