package org.keedio.storm.bolt.filterkey.bolt.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


import java.util.Map;

/**
 * Created by luislazaro on 27/5/15.
 * lalazaro@keedio.com
 * Keedio
 */
public class SimpleSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private String[] events = {
            "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                    ",\"message\":\"the original body string of event A\"}",
            "{\"extraData\":{\"Delivery\":\"London\",\"Hostname\":\"host2\",\"Item\":\"fw\",\"Ciid\":\"435\"}" +
                    ",\"message\":\"the original body string of event B\"}",
            "{\"extraData\":{\"Delivery\":\"Madrid\",\"Hostname\":\"host3\",\"Item\":\"switch\",\"Ciid\":\"421\"}" +
                    ",\"message\":\"the original body string of event C\"}",
            "{\"extraData\":{\"Delivery\":\"Paris\",\"Hostname\":\"host4\",\"Item\":\"router\",\"Ciid\":\"765\"}" +
                    ",\"message\":\"the original body string of event D\"}",
            "{\"extraData\":{\"Delivery\":\"Oslo\",\"Hostname\":\"host5\",\"Item\":\"hub\",\"Ciid\":\"123\"}" +
                    ",\"message\":\"the original body string of event E\"}"
    };
    private int index = 0;

    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("event"));
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector
            collector){
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        while(index < events.length) {
            this.collector.emit(new Values(events[index]));
            index++;
        }
        Utils.sleep(1);
    }

    @Override
    public void ack(Object o) {
    }

    @Override
    public void fail(Object o) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
