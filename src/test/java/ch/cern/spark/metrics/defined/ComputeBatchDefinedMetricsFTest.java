package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.junit.Test;

import ch.cern.Properties.PropertiesCache;
import ch.cern.PropertiesTest;
import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeBatchDefinedMetricsFTest {

	@Test
	public void aggregateCountUpdate() throws Exception {
		PropertiesCache props = PropertiesTest.mockedExpirable();
		
		props = PropertiesTest.mockedExpirable();
		props.get().setProperty("metrics.define.dmID1.metrics.groupby", "DB_NAME, METRIC_NAME");
		props.get().setProperty("metrics.define.dmID1.variables.value.aggregate", "count");
		props.get().setProperty("metrics.define.dmID1.variables.value.expire", "5m");
		props.get().setProperty("metrics.define.dmID1.when", "batch");
		DefinedMetrics definedMetrics = new DefinedMetrics(props);
		
		Instant now = Instant.now();
		
		ComputeBatchDefineMetricsF func = new ComputeBatchDefineMetricsF(definedMetrics, new Time(now.toEpochMilli()));
		
		DefinedMetricID id = new DefinedMetricID("dmID1", new HashMap<>());
		State<DefinedMetricStore> status = new StateImpl<>();
		DefinedMetricStore state = new DefinedMetricStore();
		Map<String, String> ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1");
		ids.put("METRIC_NAME", "Read");
		state.updateAggregatedValue("value", ids.hashCode(), 0f, now);
		status.update(state);
		Iterator<Metric> result = func.call(new Tuple2<DefinedMetricID, DefinedMetricStore>(id, status.get()));
		result.hasNext();
		assertEquals(1, result.next().getValue(), 0.001f);
		
		id = new DefinedMetricID("dmID1", new HashMap<>());
		state = new DefinedMetricStore();
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2");
		ids.put("METRIC_NAME", "Read");
		state.updateAggregatedValue("value", ids.hashCode(), 0f, now);
		status.update(state);
		result = func.call(new Tuple2<DefinedMetricID, DefinedMetricStore>(id, status.get()));
		result.hasNext();
		assertEquals(1, result.next().getValue(), 0.001f);
		
		id = new DefinedMetricID("dmID1", new HashMap<>());
		id = new DefinedMetricID("dmID1", new HashMap<>());
		state = new DefinedMetricStore();
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_1");
		ids.put("METRIC_NAME", "Read");
		state.updateAggregatedValue("value", ids.hashCode(), 0f, now);
		status.update(state);
		result = func.call(new Tuple2<DefinedMetricID, DefinedMetricStore>(id, status.get()));
		result.hasNext();
		assertEquals(1, result.next().getValue(), 0.001f);
		
		id = new DefinedMetricID("dmID1", new HashMap<>());
		id = new DefinedMetricID("dmID1", new HashMap<>());
		state = new DefinedMetricStore();
		ids = new HashMap<>();
		ids.put("DB_NAME", "DB1");
		ids.put("INSTANCE_NAME", "DB1_2");
		ids.put("METRIC_NAME", "Read");
		state.updateAggregatedValue("value", ids.hashCode(), 0f, now);
		status.update(state);
		result = func.call(new Tuple2<DefinedMetricID, DefinedMetricStore>(id, status.get()));
		result.hasNext();
		assertEquals(1, result.next().getValue(), 0.001f);
	}
	
}
