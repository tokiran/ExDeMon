package ch.cern.spark.metrics.defined.equation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public class EquationTest {

	@Test
	public void evalWithLiterals() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		
		assertEquals(30f, new Equation("(5+10)*2", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(45f, new Equation("(5+10) * (3)", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(35f, new Equation("5+10 * (3)", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(-2.5f, new Equation("(5-10)/2", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(-2.5f, new Equation("(5-+10)/2", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(7.5f, new Equation("(5--10)/2", props).compute(null, null).getAsFloat().get(), 0.000f);
		
		assertTrue(new Equation("true && true", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("true || false", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("false || true", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("false || false", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("!false || !false", props).compute(null, null).getAsBoolean().get());
		assertEquals(1f, new Equation("if_float(true, 1, 2)", props).compute(null, null).getAsFloat().get(), 0f);
		assertEquals(2f, new Equation("if_float(false, 1, 2)", props).compute(null, null).getAsFloat().get(), 0f);
		assertTrue(new Equation("if_bool(true, true, false)", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("if_bool(false, true, false)", props).compute(null, null).getAsBoolean().get());
		assertEquals("true", new Equation("if_string(true, \"true\", \"false\")", props).compute(null, null).getAsString().get());
		assertEquals("false", new Equation("if_string(false, \"true\", \"false\")", props).compute(null, null).getAsString().get());
		
		assertTrue(new Equation("true == true", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("1.23 == 1.23", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"aa\" == \"aa\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\\"a\" == \"a\\\"a\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\'a\\\'a\' == \'a\\\'a\'", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\a\" == \"a\\a\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("true == 34", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("true == false", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("1.23 == 1.2343", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("1.23 == \"aa\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\"aa\" == \"bb\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\"a\\\"a\" == \"b\\\"b\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\'a\\\'a\' == \'b\\\'b\'", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\"a\\a\" == \"b\\b\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("true != 34", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("true != false", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("1.23 != 1.2343", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("1.23 != \"aa\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"aa\" != \"bb\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\\"a\" != \"b\\\"b\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\'a\\\'a\' != \'b\\\'b\'", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\a\" != \"b\\b\"", props).compute(null, null).getAsBoolean().get());
		
		String inputToTrim = "  a abc cde zz ";
		assertEquals(inputToTrim.trim(), new Equation("trim(\"" + inputToTrim + "\")", props).compute(null, null).getAsString().get());
		
		String inputToConcat1 = "aa";
		String inputToConcat2 = "bb";
		assertEquals(inputToConcat1 + inputToConcat2, new Equation("concat(\"" + inputToConcat1 + "\", \"" + inputToConcat2 + "\")", props).compute(null, null).getAsString().get());
	}
	
	@Test
	public void evalWithVariables() throws ParseException, ConfigurationException {
		Instant time = Instant.now();
		Properties props = new Properties();
		
		VariableStatuses stores = new VariableStatuses();
		ValueHistory.Status var1store = new ValueHistory.Status(100, 0, null, null);
		stores.put("var1", var1store);
		ValueHistory.Status var2store = new ValueHistory.Status(100, 0, null, null);
		stores.put("var2", var2store);
		ValueHistory.Status xstore = new ValueHistory.Status(100, 0, null, null);
		stores.put("x", xstore);
		ValueHistory.Status ystore = new ValueHistory.Status(100, 0, null, null);
		stores.put("y", ystore);
		
		props = new Properties();
		props.setProperty("var1.filter.attribute.A", "A");
		var1store.history.add(time, new FloatValue(3));	
		assertEquals(39f, new Equation("(var1+10) * (var1)", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		var1store.history.add(time, new FloatValue(3));	
		assertEquals(27f, new Equation("var1 ^ var1", props).compute(stores, time).getAsFloat().get(), 0.000f);
		assertEquals(27f, new Equation("3 ^ var1", props).compute(stores, time).getAsFloat().get(), 0.000f);
		assertEquals(27f, new Equation("var1 ^ 3", props).compute(stores, time).getAsFloat().get(), 0.000f);
	
		var1store.history.add(time, new FloatValue(3));	
		assertEquals(3f, new Equation("var1", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		props.setProperty("var2.filter.attribute.B", "B");
		var1store.history.add(time, new FloatValue(5));
		var2store.history.add(time, new FloatValue(10));
		assertEquals(35f, new Equation("var1 + var2 * (3)", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		var1store.history.add(time, new FloatValue(5));
		var2store.history.add(time, new FloatValue(10));
		assertTrue(new Equation("(var1 == 5) && (var2 == 10) ", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == 5) && ((var2 * 2) == 20)", props).compute(stores, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 == 5) && ((var2 * 2 + 1) == 20)", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == 5) && !((var2 * 2 + 1) == 20)", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var2 == 10) && ((var2 * 2) == 20)", props).compute(stores, time).getAsBoolean().get());
		
		var1store.history.add(time, new StringValue("/tmp/"));
		var2store.history.add(time, new FloatValue(10));
		assertTrue(new Equation("(var1 == '/tmp/') && (var2 == 10)", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == \"/tmp/\") && (var2 == 10)", props).compute(stores, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 != \"/tmp/\") && (var2 == 10)", props).compute(stores, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 == \"/tmp/\") && (var2 == 11)", props).compute(stores, time).getAsBoolean().get());
		
		props.setProperty("x.filter.attribute.B", "B");
		props.setProperty("y.filter.attribute.B", "B");
		xstore.history.add(time, new FloatValue(5));
		ystore.history.add(time, new StringValue("10"));
		Value result = new Equation("x + y", props).compute(stores, Instant.now());
		assertTrue(result.getAsException().isPresent());
		assertEquals("Function \"+\": argument 2: requires float value", result.getAsException().get());
		assertEquals("(last(var(x))=5.0 + last(var(y))=\"10\")={Error: argument 2: requires float value}", result.getSource());
		
		xstore.history.add(time, new FloatValue(5));
        ystore.history.add(time, new FloatValue(10));
		var1store.history.add(time, new FloatValue(10));
		assertEquals(105f, new Equation("x+y * (var1)", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		xstore.history.add(time, new FloatValue(5));
        ystore.history.add(time, new FloatValue(10));
		var1store.history.add(time, new BooleanValue(true));
		var2store.history.add(time, new BooleanValue(false));
		assertTrue(new Equation("(x + y) == 15 && (var1 == true) && (var2 != true)", props).compute(stores, time).getAsBoolean().get());
		assertEquals(5, new Equation("if_float(var1, x, y)", props).compute(stores, time).getAsFloat().get(), 0f);
		assertEquals(10, new Equation("if_float(var2, x, y)", props).compute(stores, time).getAsFloat().get(), 0f);
		assertEquals(15, new Equation("if_float(var1, x, 0) + if_float(!var2, y, 0)", props).compute(stores, time).getAsFloat().get(), 0f);
		assertEquals(15, new Equation("if_float(var1, x, y) + if_float(!var1, x, y)", props).compute(stores, time).getAsFloat().get(), 0f);
		
		String inputToConcat1 = "aa";
		String inputToConcat2 = "bb";
		var1store.history.add(time, new StringValue(inputToConcat1));
		var2store.history.add(time, new StringValue(inputToConcat2));
		assertEquals(inputToConcat1 + inputToConcat2, new Equation("concat(var1, var2)", props).compute(stores, time).getAsString().get());
	}

	@Test
	public void evalWithVariablesAndFormulas() throws ParseException, ConfigurationException {
		Instant time = Instant.now();
		Properties props = new Properties();;
		props.setProperty("x.filter.attribute.A", "A");
		
		VariableStatuses stores = new VariableStatuses();
		ValueHistory.Status xstore = new ValueHistory.Status(100, 0, null, null);
        stores.put("x", xstore);
        ValueHistory.Status ystore = new ValueHistory.Status(100, 0, null, null);
        stores.put("y", ystore);
		
		xstore.history.add(time, new FloatValue(9));
		assertEquals(9f, new Equation("abs(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		xstore.history.add(time, new FloatValue(-9));
		assertEquals(9f, new Equation("abs(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.history.add(time, new FloatValue(10));
		assertEquals(3.16f, new Equation("sqrt(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.history.add(time, new FloatValue(10));
		assertEquals(0.17f, new Equation("sin(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.history.add(time, new FloatValue(10));
		assertEquals(0.98f, new Equation("cos(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.history.add(time, new FloatValue(10));
		assertEquals(0.17f, new Equation("tan(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		props.setProperty("y.filter.attribute.A", "A");
		xstore.history.add(time, new FloatValue(10));
		ystore.history.add(time, new FloatValue(2));
		assertEquals(2.57f, new Equation("sin(x) + cos(x) + sqrt(y)", props).compute(stores, time).getAsFloat().get(), 0.01f);
	}
	
}
