
package org.keedio.storm.filterkey.services;


import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.TreeMap;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author Luis Lázaro <lalazaro@keedio.com>
 */
public class CriteriaFilterTest {
    
    /*
     Test the constructor of the class
    */

    @Test
    public void testCreateFilter() throws Exception {
        System.out.println("createFilter");

        String expResult = "{\"key\":{\"Delivery\":\"Boadilla\",\"FunctionalEnvironment\":\"Production\"},"
                + "\"values\":[\"Company\",\"VirtualDC\",\"Manufacturer\"]}";


        TreeMap<String,String> key = new TreeMap<String,String>();
        List<String> values =new ArrayList<String>();
        key.put("Delivery", "Boadilla");
        key.put("FunctionalEnvironment", "Production");



        values.add("Company");
        values.add("VirtualDC");
        values.add("Manufacturer");

        CriteriaFilter instance = new CriteriaFilter(key, values);

        List<CriteriaFilter> resultList = new ArrayList<CriteriaFilter>();
        resultList.add(instance);


        String result = "";
        for (CriteriaFilter criterio: resultList){
            System.out.println("result is " + criterio);
            result = criterio.toString();
        }

        assertEquals(expResult, result);

    }


    /**
     * Test of createListCriteria method, of class CriteriaFilter.
     */


    @Test
    public void testCreateListCriteria() throws Exception {
        System.out.println("createListCriteria");

        Map<String, String> map = new TreeMap<String,String>();
        map.put("1", "{\"key\":{\"FunctionalEnvironment\":\"Production\",\"Delivery\":\"Boadilla\"},"
                + "\"values\":[\"Company\",\"VirtualDC\",\"Manufacturer\"]}");

        map.put("2", "{\"key\":{\"FunctionalEnvironment\":\"Production\",\"Delivery\":\"Mesena\"},"
                + "\"values\":[\"Company\",\"VirtualDC\"]}");



        String expResult1 = "{\"key\":{\"Delivery\":\"Boadilla\",\"FunctionalEnvironment\":\"Production\"},"
                + "\"values\":[\"Company\",\"VirtualDC\",\"Manufacturer\"]}";
        String expResult2 = "{\"key\":{\"Delivery\":\"Mesena\",\"FunctionalEnvironment\":\"Production\"},"
                + "\"values\":[\"Company\",\"VirtualDC\"]}";


        TreeMap<String,String> key = new TreeMap<>();
        List<String> values =new ArrayList<String>();
        CriteriaFilter instance = new CriteriaFilter(key, values);

        List<CriteriaFilter> resultList =  instance.createListCriteria(map);

        String result1 = "";
        String result2 = "";
        for (int i =  0 ; i < resultList.size() ; i++){
            result1 = resultList.get(0).toString();
            result2 = resultList.get(1).toString();
            break;
        }
        System.out.println(result1);
        System.out.println(expResult1);
        System.out.println(result2);
        System.out.println(expResult2);



        assertEquals(expResult1, result1);
        assertEquals(expResult2, result2);

    }


}