package org.keedio.storm.filterkey.services;

import com.opencsv.CSVReader;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;


import com.google.common.collect.Maps;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Filtering {

    private static final Logger logger = LoggerFactory.getLogger(Filtering.class);
    private List<CriteriaFilter> listCriterias = new ArrayList<>();


    private String PROPERTIES_CRITERIA_SELECTION = "properties.selection.criteria.";
    private String REGEXP_DIGITS = "\\d+";


    /**
     * setProperties takes mapping of properties.selection.criteria
     * according values in config's topology.
     *
     * @param config
     */
    public void setProperties(Map<String, String> config) {
        Map<String, String> criterias = getCriterias(config, PROPERTIES_CRITERIA_SELECTION);
        CriteriaFilter criteriaFilter = new CriteriaFilter();
        try {
            listCriterias = criteriaFilter.createListCriteria(criterias);
        } catch (IOException e) {
            logger.error("IO", e);
        }
    }

    /**
     * Argument pattern 'properties.criteria.selection.\\d+'
     * will be searched in config' topology. If match, key and value
     * will be collected into mapOfCriterias.
     * <p/>
     * For example, if in config exists:
     * properties.criteria.selection.0 = {"key":{"Field1":"ValueforField1","Field2":"ValueforField2"},
     * "values":["Field3","Field4","Field5"] }, field "properties.criteria.selection.0" will be
     * added as key, and "{"key":{"Field1":"ValueforField1","Field2":"ValueforField2"},
     * "values":["Field3","Field4","Field5"] }"  will be added as value to mapOfCriterias
     *
     * @param config
     * @param pattern
     * @return
     */
    public Map<String, String> getCriterias(Map config, String pattern) {
        Map<String, String> mapOfCriterias = new HashMap<>();
        Pattern pat = Pattern.compile(pattern + REGEXP_DIGITS);

        Iterator<String> it = config.keySet().iterator();

        while (it.hasNext()) {
            String key = it.next();
            Matcher m = pat.matcher(key);
            if (m.find()) {
                String value = (String) config.get(key);
                mapOfCriterias.put(key, value);
            }
        }
        return mapOfCriterias;
    }

    /**
     *
     * @param map
     * @return
     */
    public Map<String,String> filterMap(Map<String,String> map){
        Map<String,String> filteredMap = new HashMap<>();
        return filteredMap;
    }
}
