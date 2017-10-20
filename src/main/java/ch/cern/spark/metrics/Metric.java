package ch.cern.spark.metrics;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class Metric implements Serializable{

    private static final long serialVersionUID = -182236104179624396L;

    private Map<String, String> ids;
    
    private Instant timestamp;
    
    private float value;

    public Metric(Instant timestamp, float value, Map<String, String> ids){
        if(ids == null)
            this.ids = new HashMap<String, String>();
        else
            this.ids = ids;
        
        this.timestamp = timestamp;
        this.value = value;
    }
    
    public void addID(String key, String value){
        ids.put(key, value);
    }
    
    public void setIDs(Map<String, String> ids) {
        this.ids = ids;
    }

	public void removeIDs(Set<String> keySet) {
		keySet.forEach(key -> ids.remove(key));
	}
    
    public Map<String, String> getIDs(){
        return ids;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    public Instant getInstant(){
        return timestamp;
    }

    public float getValue() {
        return value;
    }
    
    public void setValue(float newValue) {
        this.value = newValue;
    }

    @Override
    public String toString() {
        return "Metric [ids=" + ids + ", timestamp=" + timestamp + ", value=" + value + "]";
    }
    
    @Override
	public Metric clone() throws CloneNotSupportedException {
    		return new Metric(timestamp, value, new HashMap<>(ids));
    }

	public<R> Optional<R> map(Function<Metric, ? extends R> mapper) {
        Objects.requireNonNull(mapper);

        return Optional.ofNullable(mapper.apply(this));
	}

}
