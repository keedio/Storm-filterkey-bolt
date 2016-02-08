package org.keedio.storm.bolt.filterkey;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Test;
import org.keedio.storm.bolt.filterkey.FilterkeyBolt;
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

    @After
    public void finish() throws IOException {
    }

    @Test
    public void testEmptyValueForCriterias(){
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        conf.put("filterkey.bolt.key.selection.criteria.1","");

        conf.put("ganglia.report", "false");

        bolt.prepare(conf, topologyContext, collector);
        System.out.println("emptyValuesforCriterias");
        Tuple tuple = mock(Tuple.class);

        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";

        when(tuple.getBinary(0)).thenReturn(event.getBytes());

        bolt.execute(tuple);
    }


    @Test
    public void testEmptyCriterias(){
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        conf.put("","");
        conf.put("ganglia.report", "false");

        bolt.prepare(conf, topologyContext, collector);
        System.out.println("emptyCriterias");
        Tuple tuple = mock(Tuple.class);

        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";

        when(tuple.getBinary(0)).thenReturn(event.getBytes());

        bolt.execute(tuple);
    }

    @Test
    public void testFilterTuple() throws InterruptedException {
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        conf.put("filterkey.bolt.key.selection.criteria.1","{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}");

        conf.put("ganglia.report", "false");

        bolt.prepare(conf, topologyContext, collector);
        System.out.println("filterTuple");
        Tuple tuple = mock(Tuple.class);

        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";


        when(tuple.getBinary(0)).thenReturn(event.getBytes());

        bolt.execute(tuple);
        
    }



    @Test
    public void testExtractExtradata() throws ParseException{
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        conf.put("filterkey.bolt.key.selection.criteria.1","{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}");

        conf.put("ganglia.report", "false");

        bolt.prepare(conf, topologyContext, collector);

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
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        conf.put("filterkey.bolt.key.selection.criteria.1","{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}");

        conf.put("ganglia.report", "false");

        bolt.prepare(conf, topologyContext, collector);

        System.out.println("test method exractMessage: ");
        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";

        String Actual = "the original body string";

        String Expected = bolt.extractMessage(event);
        System.out.println(Expected);

        assertEquals(Expected, Actual);

        System.out.println("\n");
    }

    public void testEmptyKeyForCriterias(){
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        //conf.put("filterkey.bolt.key.selection.criteria.1","{\"key\":{\"\":\"\"},\"values\":[\"Item\"]}");
        conf.put("filterkey.bolt.key.selection.criteria.1","{\"key\":{},\"values\":[\"Item\"]}");

        conf.put("ganglia.report", "false");

        bolt.prepare(conf, topologyContext, collector);
        System.out.println("emptyKeysforCriterias");
        Tuple tuple = mock(Tuple.class);

        String event = "{\"extraData\":{\"Delivery\":\"Boadilla\",\"Hostname\":\"host1\",\"Item\":\"proxy\",\"Ciid\":\"211\"}" +
                ",\"message\":\"the original body string\"}";

        when(tuple.getBinary(0)).thenReturn(event.getBytes());

        bolt.execute(tuple);
    }

    /**
     * Test get properties of ganglia reporting.
     */
    public void  testLoadGangliaProperties(){
        System.out.println("testLoadGangliaProperties");
        bolt = new FilterkeyBolt();
        Config conf = new Config();

        //key = ganglia.report and value doesnot exists in config file.
        //must be set to "no" by default and no load ganglia.
        assertFalse(bolt.loadGangliaProperties(conf));

        conf = new Config();
        //key = ganglia.report and value exists in config file and set to no.
        conf.put("ganglia.report", "false");
        assertFalse(bolt.loadGangliaProperties(conf));

        conf = new Config();
        conf.put("ganglia.report", "true");
        conf.put("ganglia.host", "localhost");
        conf.put("ganglia.port", "5555");
        conf.put("ganglia.ttl", "1");
        conf.put("ganglia.UDPAddressingMode", "UNICAST");
        conf.put("ganglia.minutes", "1");
        assert(bolt.loadGangliaProperties(conf));

        conf = new Config();
        conf.put("ganglia.report", "true");
        assert(bolt.loadGangliaProperties(conf));
    }
}
