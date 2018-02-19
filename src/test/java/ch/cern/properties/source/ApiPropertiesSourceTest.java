package ch.cern.properties.source;

import ch.cern.properties.Properties;
import ch.cern.properties.source.types.ApiPropertiesSource;
import static org.junit.Assert.assertEquals;

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
        
        assertEquals("rcount + wcount", props.get("metrics.define.tape_total_number_of_mounts.value"));
        assertEquals("24h", props.get("metrics.define.tape_media_write_error.variables.count.expire"));
    }

}
