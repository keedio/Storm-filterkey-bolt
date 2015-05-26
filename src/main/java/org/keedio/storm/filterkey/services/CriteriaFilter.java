package org.keedio.storm.filterkey.services;


import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Luis Lázaro <lalazaro@keedio.com>
 */
public class CriteriaFilter {

    private static final Logger log = LoggerFactory.getLogger(CriteriaFilter.class);
    private Map<String, String> key;
    private List<String> values;

    @JsonCreator
    public CriteriaFilter(@JsonProperty("key") TreeMap<String, String> key,
            @JsonProperty("values") List<String> values) {
        this.key = key;
        this.values = values;
    }

    public CriteriaFilter() {
    }

    ;
   
    public List<CriteriaFilter> createListCriteria(Map<String, String> map) throws IOException {
        List<CriteriaFilter> listCriteriaFilter = new ArrayList<CriteriaFilter>();
        CriteriaFilter criteriaFilter;

        for (String json : map.values()) {
            if (json != null && !json.isEmpty()) {
                criteriaFilter = JSONStringSerializer.fromJSONString(json, CriteriaFilter.class);
                listCriteriaFilter.add(criteriaFilter);
            } else {
                continue;
            }
        }
        return listCriteriaFilter;
    }

    public Map<String, String> getKey() {
        return key;
    }

    public List<String> getValues() {
        return values;
    }

    public void setKey(Map<String, String> map) {
        key = map;
    }

    public void setValuesCriteria(List<String> list) {
        values = list;
    }

    @Override
    public String toString() {
        String s = null;
        try {
            s = JSONStringSerializer.toJSONString(this);
        } catch (IOException e) {
            log.error("IO", e);
        }
        return s;
    }
}
