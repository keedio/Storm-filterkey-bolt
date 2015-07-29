package org.keedio.storm.bolt.filterkey.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * Created by luislazaro on 27/5/15.
 * lalazaro@keedio.com
 * Keedio
 */
public class ReportBoltTest implements IRichBolt {


    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    }

//    @Override
//    public void execute(Tuple tuple) {
//        try {
//            String message = new String(tuple.getBinary(0));
//            JSONParser parser = new JSONParser();
//            // El mensaje recibido es del tipo {"extraData":"...", "message":"..."}
//            JSONObject obj = (JSONObject) parser.parse(message);
//            System.out.println(obj);
//            // Aplicamos el filtro para obtener map con resultados
//
//        } catch (ParseException e){
//
//        }
//    }

    @Override
    public void execute(Tuple tuple) {
        String message = (tuple.getString(0));
        System.out.println("recibido :" + message + "\n");
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        /**
         * this bolt is final, it does not emit streams
         */
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
