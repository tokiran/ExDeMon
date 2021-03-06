package ch.cern.spark.metrics.defined.equation.var;

import java.util.Optional;
import java.util.function.Predicate;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.value.Value;

public abstract class Variable implements ValueComputable, Predicate<Metric> {
	
	protected String name;

	public Variable(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public Variable config(Properties properties, Optional<Class<? extends Value>> typeOpt) throws ConfigurationException {
		return this;
	}
	
}
