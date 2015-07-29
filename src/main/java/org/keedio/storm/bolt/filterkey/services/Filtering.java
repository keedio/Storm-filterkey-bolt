package org.keedio.storm.bolt.filterkey.services;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;


import com.google.common.collect.Maps;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Filtering {

    private static final Logger logger = LoggerFactory.getLogger(Filtering.class);
    private List<CriteriaFilter> listCriterias = new ArrayList<>();


    private String KEY_CRITERIA = "key.selection.criteria.";
    private String REGEXP_DIGITS = "\\d+"; //any combination of digits after KEY_CRITERIA


    /**
     * setProperties takes mapping of key.selection.criteria
     * according values in topology's config
     *
     * @param config Topolgys config
     */
    public void setProperties(Map<String, String> config) {
        Map<String, String> criterias = getCriterias(config, KEY_CRITERIA);
        CriteriaFilter criteriaFilter = new CriteriaFilter();
        try {
            listCriterias = criteriaFilter.createListCriteria(criterias);
        } catch (IOException e) {
            logger.error("IO", e);
        }
    }

    /**
     * Argument pattern 'key.criteria.selection.\\d+'
     * will be searched in topology's config. If match, key and value
     * will be collected into mapOfCriterias.
     * 
     * For example, if in config exists:
     * key.criteria.selection.0 = {"key":{"Field1":"ValueforField1","Field2":"ValueforField2"},
     * "values":["Field3","Field4","Field5"] }, field "key.criteria.selection.0" will be
     * added as key, and "{"key":{"Field1":"ValueforField1","Field2":"ValueforField2"},
     * "values":["Field3","Field4","Field5"] }"  will be added as value to mapOfCriterias
     *
     * @param config Topologys config
     * @param pattern to match
     * @return map of criterias
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
     * filterMap returns a reduced map of keys and values. The entries
     * in common between extradaData and criterias are checked for equality,
     * if true, get the field of the criteria a its value found in extraData and
     * accomodate in new map.
     *
     * listCriterias is a list of json. Each json is called a criteria.
     * @see "CriteriaFilter"
     *
     * @param mapOfExtradata subfield 
     * @return map filtered
     */
    public Map<String,String> filterMap(Map<String,String> mapOfExtradata){
        Map<String,String> filteredMap = new HashMap<>();
        Map<String, String> commonMap = new HashMap<>();

        for (CriteriaFilter criterio : listCriterias){
            commonMap = Maps.difference(mapOfExtradata, criterio.getKey()).entriesInCommon();
            if (criterio.getKey().equals(commonMap)){
                filteredMap.putAll(criterio.getKey());
                for (String field: criterio.getValues()){
                    filteredMap.put(field, mapOfExtradata.get(field));
                }
                return filteredMap;
            }
        }
        return filteredMap;
    }
}
