
package org.keedio.storm.bolt.filterkey.bolt.topology;

import org.keedio.storm.bolt.filterkey.bolt.FilterkeyBolt;
import org.keedio.storm.bolt.filterkey.bolt.ReportBoltTest;
import org.keedio.storm.bolt.filterkey.bolt.spout.SimpleSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import org.junit.Test;

/**
 * @author Luis Lázaro
 *         lalazaro@keedio.com
 *         Keedio
 */
public class SimpleTopology {

    private static final String EVENT_SPOUT_ID = "event-spout";
    private static final String FILTERKEY_BOLT_ID = "filterkey-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "even-topology";

    @Test
    public void testTopology() throws InterruptedException {
        SimpleSpout simpleSpout = new SimpleSpout();
        FilterkeyBolt filterkeyBolt = new FilterkeyBolt();
        ReportBoltTest reportBoltTest = new ReportBoltTest();
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(EVENT_SPOUT_ID, simpleSpout);
        builder.setBolt(FILTERKEY_BOLT_ID, filterkeyBolt).shuffleGrouping(EVENT_SPOUT_ID);
        builder.setBolt(REPORT_BOLT_ID, reportBoltTest).globalGrouping(FILTERKEY_BOLT_ID);

        Config config = new Config();
        config.put("ganglia.report", "no");
        config.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}");
        config.put("key.selection.criteria.2", "{\"key\":{\"Hostname\":\"host2\"},\"values\":[\"Ciid\"]}");
        //a failed map now, (no field for property key)
        config.put("key.selection.criteria.3", "{\"Delivery\":\"Madrid\",\"Hostname\":\"host3\",\"values\":[\"Ciid\"]}");
        config.put("key.selection.criteria.4", "{\"key\":{\"Delivery\":\"Paris\",\"Item\":\"router\" },\"values\":[\"Hostname\"]}");
        config.put("key.selection.criteria.5", "{\"key\":{\"Delivery\":\"Oslo\"},\"values\":[\"Hostname\",\"Item\"]}");
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Thread.sleep(1000);
        //cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

}
