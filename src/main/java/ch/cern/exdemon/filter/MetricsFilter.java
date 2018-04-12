package ch.cern.exdemon.filter;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;

import ch.cern.exdemon.Metric;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class MetricsFilter {
    
    private String expression;
    private Properties attributesFilterProps;

    public MetricsFilter(){
    }

    public void config(Properties props) {
        expression = props.getProperty("expr");
        attributesFilterProps = props.getSubset("attribute");
    }

    public Dataset<Metric> apply(Dataset<Metric> metrics) throws ConfigurationException {
        
        if(expression != null)
            metrics = metrics.filter(expression);
        
        for (Entry<Object, Object> attribute : attributesFilterProps.entrySet()) {
            String key = "att." + (String) attribute.getKey();
            String valueString = (String) attribute.getValue();
            
            boolean negate = valueString.startsWith("!");
            if(negate) 
                valueString = valueString.substring(1);
            
            String[] values = getValues(valueString);
            
            if(negate)
                metrics = metrics.filter(not(col(key).isin(values)));
            else
                metrics = metrics.filter(col(key).isin(values));
        }
        
        return metrics;
    }

    private static String[] getValues(String valueString) {
        Pattern pattern = Pattern.compile("([\"'])(?:(?=(\\\\?))\\2.)*?\\1");
        
        LinkedList<String> hits = new LinkedList<>();
        
        Matcher m = pattern.matcher(valueString);
        while (m.find()) {
            String value = m.group();
            
            value = value.substring(1, value.length() - 1);
            
            hits.add(value);
        }
        
        if(hits.size() == 0)
            hits.add(valueString);
        
        return hits.toArray(new String[0]);
    }
    
}
