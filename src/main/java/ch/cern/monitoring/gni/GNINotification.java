package ch.cern.monitoring.gni;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.notifications.Notification;

public class GNINotification {
	
	private Map<String, String> header;
	
	private Map<String, HashMap<String, Object>> body;

	private static Set<String> int_keys = new HashSet<>(Arrays.asList("timestamp", "metric_id", "validity", "snow_assignment_level"));

	public GNINotification() {
		this.header = new HashMap<>();
		
		this.body = new HashMap<>();
		this.body.put("metadata", new HashMap<String, Object>());
		this.body.put("payload", new HashMap<String, Object>());
		
		putInHeader("m_version", "2");
		putInHeader("m_type", "notification");
		putInMetadata("timestamp", Long.toString(Instant.now().getEpochSecond()));
		putInMetadata("uuid", UUID.randomUUID().toString());
	}
	
	private void putInHeader(String key, String value) {
		header.put(key, value);
	}
	
	private void putInMetadata(String key, String valueString) {
		Object value;
		
		if(int_keys.contains(key))
			value = Integer.parseInt(valueString);
		else
			value = valueString;
		
		body.get("metadata").put(key, value);
	}
	
	private void putInPayload(String key, String value) {
		body.get("payload").put(key, value);
	}

	public static GNINotification from(Properties properties, Notification notification) {
		GNINotification gniNotification = new GNINotification();
		
		properties = replaceValuesWithTags(properties, notification.getTags());
		
		properties.getSubset("header").entrySet().stream().forEach(entry ->{
			gniNotification.putInHeader((String) entry.getKey(), (String) entry.getValue());
		});
		
		properties.getSubset("body.metadata").entrySet().stream().forEach(entry ->{
			gniNotification.putInMetadata((String) entry.getKey(), (String) entry.getValue());
		});
		
		properties.getSubset("body.payload").entrySet().stream().forEach(entry ->{
			gniNotification.putInPayload((String) entry.getKey(), (String) entry.getValue());
		});
		
		return gniNotification;
	}

	private static Properties replaceValuesWithTags(Properties properties, Map<String, String> tags) {
		Map<String, String> newProperties = properties.toStringMap();
		
		newProperties.entrySet().stream().filter(entry -> entry.getValue().startsWith("%")).forEach(entry -> {
			String value = tags.get(entry.getValue().substring(1));
			
			if(value != null)
				newProperties.put(entry.getKey(), value);
		});
		
		properties = new Properties();
		properties.putAll(newProperties);
		
		return properties;
	}

}