package org.keedio.storm.bolt.filterkey.services;

import junit.framework.TestCase;


import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;


/**
 * Created by luislazaro on 26/5/15.
 * lalazaro@keedio.com
 * Keedio
 */


public class FilteringTest extends TestCase {

    private Filtering filtering;

    @Before
    public void setUp() throws IOException {

      filtering = new Filtering();


    }

    @Test
    public void testGetSingleCriteria(){
        System.out.println("test getCriterias single criteria:");

        Map<String, String> mapOfConfig = new HashMap<>();
        mapOfConfig.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}" );
        mapOfConfig.put("conf.pattern1","(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+");

        Map<String, String> mapOfCriteriasExpected = new HashMap<>();
        mapOfCriteriasExpected.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}" );
        System.out.println(mapOfCriteriasExpected);

        Map<String, String> mapOfCriteriasActual = filtering.getCriterias(mapOfConfig,"key.selection.criteria." );

        System.out.println(mapOfCriteriasActual);

        assertEquals(mapOfCriteriasExpected, mapOfCriteriasActual);

        System.out.println("\n");
    }

    @Test
    public void testGetMultipleCriteria(){
        System.out.println("test getCriterias multiple criterias:");

        Map<String, String> mapOfConfig = new HashMap<>();
        mapOfConfig.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}" );
        mapOfConfig.put("key.selection.criteria.2", "{\"key\":{\"Hostname\":\"host1\"},\"values\":[\"Ciid\"]}" );
        mapOfConfig.put("conf.pattern1","(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+");

        Map<String, String> mapOfCriteriasExpected = new HashMap<>();
        mapOfCriteriasExpected.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}" );
        mapOfCriteriasExpected.put("key.selection.criteria.2", "{\"key\":{\"Hostname\":\"host1\"},\"values\":[\"Ciid\"]}");
        System.out.println(mapOfCriteriasExpected);

        Map<String, String> mapOfCriteriasActual = filtering.getCriterias(mapOfConfig,"key.selection.criteria." );

        System.out.println(mapOfCriteriasActual);

        assertEquals(mapOfCriteriasExpected, mapOfCriteriasActual);

        System.out.println("\n");
    }

    @Test
    public void testFilterMapSingleCriteriaSingleKey() throws IOException{
        System.out.println("test filter of map single criteria and single key:");

        Map<String, String> mapOfConfig = new HashMap<>();
        mapOfConfig.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}" );
        mapOfConfig.put("conf.pattern1","(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+");

        filtering.setProperties(mapOfConfig);

        Map<String,String> mapOfExtradata = new HashMap<>();
        mapOfExtradata.put("Delivery","Boadilla");
        mapOfExtradata.put("Hostname","host1");
        mapOfExtradata.put("Item", "proxy");
        mapOfExtradata.put("Ciid", "211");

        Map<String, String> mapFilteredExpected = new HashMap<>();
        mapFilteredExpected.put("Delivery", "Boadilla");
        mapFilteredExpected.put("Item", "proxy");

        Map<String, String> mapFilteredActual;
        mapFilteredActual = filtering.filterMap(mapOfExtradata);

        System.out.println("extradata to be filtered: " + mapOfExtradata);
        System.out.println("map de criterias to be used as criteria filter: " +
                "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}" );
        System.out.println("filtered map result: " + mapFilteredActual);

        assertEquals(mapFilteredExpected, mapFilteredActual);

        System.out.println("\n");
    }

    @Test
    public void testFilterMapSingleCriteriaMultipleFields() throws IOException{
        System.out.println("test filter of map single criteria multiple fields:");

        Map<String, String> mapOfConfig = new TreeMap<>();
        mapOfConfig.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\",\"Company\"]}" );

        mapOfConfig.put("conf.pattern1","(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+");

        filtering.setProperties(mapOfConfig);

        Map<String,String> mapOfExtradata = new TreeMap<>();
        mapOfExtradata.put("Delivery","Boadilla");
        mapOfExtradata.put("Hostname","host1");
        mapOfExtradata.put("Item", "proxy");
        mapOfExtradata.put("Ciid", "211");
        mapOfExtradata.put("Company", "totta");
        mapOfExtradata.put("Type", "logical server");

        Map<String, String> mapFilteredExpected = new TreeMap<>();
        mapFilteredExpected.put("Delivery", "Boadilla");
        mapFilteredExpected.put("Item", "proxy");
        mapFilteredExpected.put("Company", "totta");

        Map<String, String> mapFilteredActual;
        mapFilteredActual = filtering.filterMap(mapOfExtradata);

        System.out.println("extradata to be filtered: " + mapOfExtradata);

        System.out.println("map de criterias to be used as criteria filter: " +
                "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\", \"Company\"]}" );

        System.out.println("filtered map result: " + mapFilteredActual);

        assertEquals(mapFilteredExpected, mapFilteredActual);

        System.out.println("\n");
    }

    @Test
    public void testFilterMapSingleCriteriaMultipleKeys() throws IOException{
        System.out.println("test filter of map single criteria multiple keys:");

        Map<String, String> mapOfConfig = new TreeMap<>();
        mapOfConfig.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Boadilla\",\"Company\":\"totta\"},\"values\":[\"Item\"]}" );

        mapOfConfig.put("conf.pattern1","(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+");

        filtering.setProperties(mapOfConfig);

        Map<String,String> mapOfExtradata = new TreeMap<>();
        mapOfExtradata.put("Delivery","Boadilla");
        mapOfExtradata.put("Hostname","host1");
        mapOfExtradata.put("Item", "proxy");
        mapOfExtradata.put("Ciid", "211");
        mapOfExtradata.put("Company", "totta");
        mapOfExtradata.put("Type", "logical server");

        Map<String, String> mapFilteredExpected = new TreeMap<>();
        mapFilteredExpected.put("Delivery", "Boadilla");
        mapFilteredExpected.put("Item", "proxy");
        mapFilteredExpected.put("Company", "totta");

        Map<String, String> mapFilteredActual;
        mapFilteredActual = filtering.filterMap(mapOfExtradata);

        System.out.println("extradata to be filtered: " + mapOfExtradata);

        System.out.println("map de criterias to be used as criteria filter: " +
                "{\"key\":{\"Delivery\":\"Boadilla\",\"Company\":\"totta\" },\"values\":[\"Item\"]}" );

        System.out.println("filtered map result: " + mapFilteredActual);

        assertEquals(mapFilteredExpected, mapFilteredActual);

        System.out.println("\n");
    }

    @Test
    public void testFilterMapMultipleCriteriaSingleKey() throws IOException{
        System.out.println("test filter of map Multiple criteria and single key:");

        Map<String, String> mapOfConfig = new HashMap<>();
        mapOfConfig.put("key.selection.criteria.1", "{\"key\":{\"Delivery\":\"Dublin\"},\"values\":[\"Item\"]}" );
        mapOfConfig.put("key.selection.criteria.2", "{\"key\":{\"Company\":\"totta\"},\"values\":[\"Type\"]}" );
        mapOfConfig.put("conf.pattern1","(<date>[^\\s]+)\\s+(<time>[^\\s]+)\\s+");

        filtering.setProperties(mapOfConfig);

        Map<String,String> mapOfExtradata = new TreeMap<>();
        mapOfExtradata.put("Delivery","Boadilla");
        mapOfExtradata.put("Hostname","host1");
        mapOfExtradata.put("Item", "proxy");
        mapOfExtradata.put("Ciid", "211");
        mapOfExtradata.put("Company", "totta");
        mapOfExtradata.put("Type", "logical server");

        Map<String, String> mapFilteredExpected = new HashMap<>();
        mapFilteredExpected.put("Company", "totta");
        mapFilteredExpected.put("Type", "logical server");

        Map<String, String> mapFilteredActual;
        mapFilteredActual = filtering.filterMap(mapOfExtradata);

        System.out.println("extradata to be filtered: " + mapOfExtradata);
        //System.out.println("map de criterias to be used as criteria filter: " +
        //  "{\"key\":{\"Delivery\":\"Boadilla\"},\"values\":[\"Item\"]}" );
        System.out.println("filtered map result: " + mapFilteredActual);

        assertEquals(mapFilteredExpected, mapFilteredActual);

        System.out.println("\n");
    }

}
