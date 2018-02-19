package ch.cern.properties.source;

import ch.cern.properties.Properties;
import ch.cern.properties.source.types.ApiPropertiesSource;

import org.junit.Before;
import org.junit.Test;


public class ApiPropertiesSourceTest {
	
    @Before
    public void setUp() throws Exception {
        Properties.initCache(null);
        Properties.getCache().reset();
    }
    
    @Test
    public void loadFromAPI() throws Exception {
        ApiPropertiesSource apiprops = new ApiPropertiesSource();
        Properties props = apiprops.load();
        
        System.out.println("PROPS:");
        System.out.println(props);
    }

    /*@Test
    public void fromJSON() {
        String jsonString = "{\"metrics.schema.perf\":{"
                        + "\"sources\":\"tape_logs\", "
                        + "\"filter.attribute\":\"1234\"}"
                        + "}";

        JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();

    Properties props = Properties.from(jsonObject);

    assertEquals("tape_logs", props.get("metrics.schema.perf.sources"));
    assertEquals("1234", props.get("metrics.schema.perf.filter.attribute"));
    }*/
	
}
