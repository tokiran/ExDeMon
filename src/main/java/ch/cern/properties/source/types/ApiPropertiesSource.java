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
        
        System.out.println("CARGANDO AQUI");
    }

    @Override
    public Properties load() throws Exception {
        Properties props = new Properties();

        try {
            //props = loadFromUrl(props, "http://dbodtests.cern.ch:5000/api/v1/schemas");
            //props = loadFromUrl(props, "http://dbodtests.cern.ch:5000/api/v1/metrics");
            //props = loadFromUrl(props, "http://dbodtests.cern.ch:5000/api/v1/monitors");
            loadMetrics(props);
        } catch(Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
        
        return props;
    }
    
    private JsonObject loadFromUrl(String url) throws RuntimeException {
        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse response = webResource.get(ClientResponse.class);

        if (response.getStatus() != 200) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
        }

        String output = response.getEntity(String.class);
        System.out.println("Output from Server .... \n");
        System.out.println(output);
        //Properties fileProps = Properties.fromJson(output);
        //props.putAll(fileProps);
        System.out.println("LOADED!");
        //return props;
        JsonParser jparser = new JsonParser();
        return jparser.parse(output).getAsJsonObject();
    }
    
    private void loadMetrics(Properties props) throws Exception {
        JsonParser jparser = new JsonParser();
        
        JsonArray metrics = loadFromUrl("http://dbodtests.cern.ch:5000/api/v1/metrics").getAsJsonArray("metrics");
        Iterator<JsonElement> metricsItr = metrics.iterator();
        while (metricsItr.hasNext()) {
            JsonObject metric = metricsItr.next().getAsJsonObject();
            JsonObject jobject = new JsonObject();
            jobject.add("metrics.define." + metric.get("name").getAsString(), metric.getAsJsonObject("data"));
            System.out.println("PARSED OBJECT: " + jobject);
            props.addFrom(jobject);
        }
        
        System.out.println(props);
    }

}
