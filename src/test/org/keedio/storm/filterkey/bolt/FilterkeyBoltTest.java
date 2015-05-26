package org.keedio.storm.filterkey.bolt;

import junit.framework.TestCase;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * Created by luislazaro on 23/5/15.
 * lalazaro@keedio.com
 * Keedio
 */


public class FilterkeyBoltTest extends TestCase {


    private FilterkeyBolt bolt;

    @Mock
    private TopologyContext topologyContext = mock(TopologyContext.class);

    @Mock
    private OutputCollector collector = mock(OutputCollector.class);

    @Before
    public void setUp() throws IOException {
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        conf.put("key.selection.criteria.1","{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}");

        bolt.prepare(conf, topologyContext, collector);
    }

    @After
    public void finish() throws IOException {
    }

    @Test
    public void testFilterTuple() throws InterruptedException {
        Tuple tuple = mock(Tuple.class);

        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";


        when(tuple.getString(anyInt())).thenReturn(event);

        bolt.execute(tuple);
    }


    @Test
    public void testExtractExtradata() throws ParseException{
        System.out.println("test method exractExtraData:");

        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";

        Map<String, String> mapExpected = new HashMap<>();
        mapExpected.put("Delivery", "Boadilla");
        mapExpected.put("Hostname", "host1");

        mapExpected.put("Ciid","211");
        mapExpected.put("Item", "proxy");

        Iterator<String> iterator = mapExpected.keySet().iterator();
        while(iterator.hasNext()){
            String key = iterator.next();
            String value = mapExpected.get(key);
            System.out.println(key + ", " + value);
        }
        System.out.println("\n");


        Map<String, String> map = bolt.extractExtradata(event);

        Iterator<String> it = map.keySet().iterator();
        while(it.hasNext()){
            String key = it.next();
            String value = mapExpected.get(key);
            System.out.println(key + ", " + value);
        }

        assertEquals(mapExpected, map);

        System.out.println("\n");
    }

    @Test
    public void testExtractMessage() throws ParseException{
        System.out.println("test method exractMessage: ");
        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";

        String Actual = "the original body string";

        String Expected = bolt.extractMessage(event);
        System.out.println(Expected);

        assertEquals(Expected, Actual);

        System.out.println("\n");
    }


}