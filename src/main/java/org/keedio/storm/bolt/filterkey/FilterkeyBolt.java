package org.keedio.storm.bolt.filterkey;

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
	private MetricsController mc;
	private int refreshTime;
	private Date lastExecution = new Date();

	// for Ganglia only
	private String hostGanglia, reportGanglia;
	private GMetric.UDPAddressingMode modeGanglia;
	private int portGanglia, ttlGanglia;
	private int minutesGanglia;

	@Override
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		filtering = new Filtering();
		filtering.setProperties(config);

		if (config.get("refreshtime") == null)
			refreshTime = 10;
		else
			refreshTime = Integer.parseInt((String) config.get("refreshtime"));

		// check if in topology's config ganglia.report is set to "true"
		if (loadGangliaProperties(config)) {
			mc = new MetricsController(hostGanglia, portGanglia, modeGanglia,
					ttlGanglia, minutesGanglia);
		} else {
			mc = new MetricsController();
		}

		mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "filtered"));
		mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "unfiltered"));
		mc.manage(new MetricsEvent(MetricsEvent.NEW_METRIC_METER, "errorFilter"));

		// Registramos la metrica para su publicacion
		SimpleMetric filtered = new SimpleMetric(mc.getMetrics(), "filtered",
				SimpleMetric.TYPE_METER);
		SimpleMetric unfiltered = new SimpleMetric(mc.getMetrics(),
				"unfiltered", SimpleMetric.TYPE_METER);
		SimpleMetric errorFilter = new SimpleMetric(mc.getMetrics(),
				"errorFilter", SimpleMetric.TYPE_METER);
		SimpleMetric histogram = new SimpleMetric(mc.getMetrics(), "histogram",
				SimpleMetric.TYPE_HISTOGRAM);

		context.registerMetric("filtered", filtered, refreshTime);
		context.registerMetric("unfiltered", unfiltered, refreshTime);
		context.registerMetric("errorFilter", errorFilter, refreshTime);
		context.registerMetric("histogram", histogram, refreshTime);
	}

	@Override
	public void execute(Tuple input) {
		// Añadimos al throughput e inicializamos el date
		Date actualDate = new Date();
		long aux = (actualDate.getTime() - lastExecution.getTime()) / 1000;
		lastExecution = actualDate;

		// Registramos para calculo de throughput
		mc.manage(new MetricsEvent(MetricsEvent.UPDATE_THROUGHPUT, aux));

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String event = new String(input.getBinary(0));
		try {
			Map<String, String> extraDataFiltered = filtering.filterMap(extractExtradata(event));

			// if allExtraData is cleaned the original message will be sent
			if (extraDataFiltered.isEmpty()) {
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "unfiltered"));
				//collector.emit(new Values(event.getBytes()));
				this.collector.emit(new Values(new Object[] { gson.toJson(event).getBytes() }));
				collector.ack(input);
			}else{
			
				mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "filtered"));
				
				Map<String, Object> finalEvent = new LinkedHashMap<String, Object>();
	
				finalEvent.put("extraData", extraDataFiltered);
				finalEvent.put("message", extractMessage(event));
					
				this.collector.emit(new Values(new Object[] { gson.toJson(finalEvent, Map.class).getBytes() }));
				collector.ack(input);
			}
		} catch (ParseException e) {
			LOGGER.error("", e);
			collector.fail(input);
			mc.manage(new MetricsEvent(MetricsEvent.INC_METER, "errorFilter"));
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

	/**
	 * ganglia's server properties are taken from main topology's config
	 *
	 * @param stormConf
	 * @return
	 */
	public boolean loadGangliaProperties(Map stormConf) {

		if (stormConf.containsKey("ganglia.report")) {
			reportGanglia = (String) stormConf.get("ganglia.report");
			if (reportGanglia.equals("true")) {
				if (stormConf.containsKey("ganglia.host"))
					hostGanglia = (String) stormConf.get("ganglia.host");
				else {
					LOGGER.warn("ganglia.host value not found in config file, using default 239.2.11.71 multicast ip");
					hostGanglia = "239.2.11.71";
				}
				if (stormConf.containsKey("ganglia.port"))
					portGanglia = Integer.parseInt((String) stormConf
							.get("ganglia.port"));
				else {
					LOGGER.warn("ganglia.port value not found in config file, using default 8649");
					portGanglia = 8649;
				}
				if (stormConf.containsKey("ganglia.ttl"))
					ttlGanglia = Integer.parseInt((String) stormConf
							.get("ganglia.ttl"));
				else {
					LOGGER.warn("ganglia.ttl value not found in config file, using default 64");
					ttlGanglia = 64;
				}

				if (stormConf.containsKey("ganglia.minutes"))
					minutesGanglia = Integer.parseInt((String) stormConf
							.get("ganglia.minutes"));
				else {
					LOGGER.warn("ganglia.minutes value not found in config file, using default 1");
					ttlGanglia = 1;
				}
				if (stormConf.containsKey("ganglia.UDPAddressingMode")) {
					String stringModeGanglia = (String) stormConf
							.get("ganglia.UDPAddressingMode");
					if (stringModeGanglia.equalsIgnoreCase("unicast"))
						modeGanglia = GMetric.UDPAddressingMode.UNICAST;
					else
						modeGanglia = GMetric.UDPAddressingMode.MULTICAST;
				} else {
					LOGGER.warn("ganglia.UDPAddressingMode value not found in config file, using default MULTICAST");
					modeGanglia = GMetric.UDPAddressingMode.MULTICAST;
				}
				return true;
			}
		}
		return false;
	}
}
