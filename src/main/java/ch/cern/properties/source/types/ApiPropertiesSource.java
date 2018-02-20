package ch.cern.properties.source.types;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;
import com.google.gson.*;
import java.util.Iterator;

@RegisterComponent("api")
public class ApiPropertiesSource extends PropertiesSource {
    
    private transient final static Logger LOG = Logger.getLogger(ApiPropertiesSource.class.getName());

	private static final long serialVersionUID = -2444021351363428469L;
	
	private String api_url;

    private transient FileSystem fs;
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
        api_url = properties.getProperty("api");
        
        properties.confirmAllPropertiesUsed();
    }

    @Override
    public Properties load() throws Exception {
        Properties props = new Properties();

        try {
            loadSchemas(props);
            loadMetrics(props);
            loadMonitors(props);
        } catch(Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
        
        return props;
    }
    
    protected JsonObject loadFromUrl(String url) throws RuntimeException {
        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse response = webResource.get(ClientResponse.class);

        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
        }

        String output = response.getEntity(String.class);
        JsonParser jparser = new JsonParser();
        return jparser.parse(output).getAsJsonObject();
    }
    
    protected void loadMetrics(Properties props) throws Exception {
        JsonArray metrics = loadFromUrl("http://dbodtests.cern.ch:5000/api/v1/metrics").getAsJsonArray("metrics");
        Iterator<JsonElement> metricsItr = metrics.iterator();
        while (metricsItr.hasNext()) {
            JsonObject metric = metricsItr.next().getAsJsonObject();
            JsonObject jobject = new JsonObject();
            jobject.add("metrics.define." + metric.get("name").getAsString(), metric.getAsJsonObject("data"));
            props.addFrom(jobject);
        }
    }
    
    protected void loadSchemas(Properties props) throws Exception {
        JsonArray schemas = loadFromUrl("http://dbodtests.cern.ch:5000/api/v1/schemas").getAsJsonArray("schemas");
        Iterator<JsonElement> schemasItr = schemas.iterator();
        while (schemasItr.hasNext()) {
            JsonObject schema = schemasItr.next().getAsJsonObject();
            JsonObject jobject = new JsonObject();
            jobject.add("metrics.schema." + schema.get("name").getAsString(), schema.getAsJsonObject("data"));
            props.addFrom(jobject);
        }
    }
    
    protected void loadMonitors(Properties props) throws Exception {
        JsonArray monitors = loadFromUrl("http://dbodtests.cern.ch:5000/api/v1/monitors").getAsJsonArray("monitors");
        Iterator<JsonElement> monitorsItr = monitors.iterator();
        while (monitorsItr.hasNext()) {
            JsonObject monitor = monitorsItr.next().getAsJsonObject();
            JsonObject jobject = new JsonObject();
            jobject.add("monitor." + monitor.get("name").getAsString(), monitor.getAsJsonObject("data"));
            props.addFrom(jobject);
        }
    }

}
