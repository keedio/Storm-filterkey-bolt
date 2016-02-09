package org.keedio.storm.bolt.filterkey;

import org.keedio.storm.bolt.filterkey.services.Filtering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author Luis Lázaro lalazaro@keedio.com Keedio
 * @author Marcelo Valle mvalle@keedio.com Keedio
 */
public class FilterkeyBolt implements IRichBolt {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(FilterkeyBolt.class);

	private Filtering filtering;
	private OutputCollector collector;

	@Override
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		filtering = new Filtering();
		filtering.setProperties(config);

	}

	@Override
	public void execute(Tuple input) {

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String event = new String(input.getBinary(0));
		try {
			Map<String, String> extraDataFiltered = filtering.filterMap(extractExtradata(event));

			// if allExtraData is cleaned the original message will be sent
			if (extraDataFiltered.isEmpty()) {
				this.collector.emit(new Values(new Object[] { gson.toJson(event).getBytes() }));
				collector.ack(input);
			}else{
				
				Map<String, Object> finalEvent = new LinkedHashMap<String, Object>();
	
				finalEvent.put("extraData", extraDataFiltered);
				finalEvent.put("message", extractMessage(event));
					
				this.collector.emit(new Values(new Object[] { gson.toJson(finalEvent, Map.class).getBytes() }));
				collector.ack(input);
			}
		} catch (ParseException e) {
			LOGGER.error("Error parsing tuple ", event);
			collector.ack(input);
		}
	}

	/**
	 * Extract named field "extraData" and return a map of its values.
	 *
	 * @param event
	 *            of tuple
	 * @return map extracted
	 * @throws ParseException
	 *             exception
	 */
	@SuppressWarnings("unchecked")
	public Map<String, String> extractExtradata(String event)
			throws ParseException {
		
		JSONParser parser = new JSONParser();
		JSONObject jsonObj = (JSONObject) parser.parse(event);
		JSONObject jsonObjExtraData = (JSONObject) jsonObj.get("extraData");
		return (Map<String,String>) jsonObjExtraData;

	}

	/**
	 * extract named field "message" and return body without changes
	 *
	 * @param event
	 *            string
	 * @return string
	 * @throws ParseException
	 *             parseException
	 */
	public String extractMessage(String event) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(event);
		return (String) obj.get("message");
	}

	/**
	 * Make a Json with a property called extraData and a value cotaining a map.
	 *
	 * @param map
	 *            of data
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
	 * Make a Json with a property called "message" and a value containig a
	 * String
	 *
	 * @param mes
	 *            message
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
}
