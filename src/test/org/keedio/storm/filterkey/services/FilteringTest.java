package org.keedio.storm.filterkey.services;

import junit.framework.TestCase;


import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.mockito.Mockito.*;


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
    public void testFilterMap() throws IOException{
        System.out.println("test filter of map");


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

        assertEquals(mapFilteredExpected, mapFilteredActual);
    }

}
