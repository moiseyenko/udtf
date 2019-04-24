package com.epam.bigdata.parse_ua_udtf;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

public class ParseUserAgentUADTFTest {

	ParseUserAgentUADTF parseUserAgentUADTF;
	
	@Before
	public void init() {
		parseUserAgentUADTF = new ParseUserAgentUADTF();
	}
	
	
	@Test
	public void testInit() throws UDFArgumentException {
		
		List<String> fieldNames = Collections.singletonList("UserAgent");
		List<ObjectInspector> fieldOIs = Collections.singletonList(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		StructObjectInspector argOIs = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
		StructObjectInspector structObjectInspector = parseUserAgentUADTF.initialize(argOIs);
		List<? extends StructField> inputFields = structObjectInspector.getAllStructFieldRefs();
		assertEquals(3, inputFields.size());
	}
	
		
	@Test(expected=UDFArgumentException.class)
	public void testInvalidInit() throws UDFArgumentException {
		List<String> fieldNames = Arrays.asList("UA1", "UA2");
		List<ObjectInspector> fieldOIs = Arrays.asList(PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		StructObjectInspector argOIs = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
		parseUserAgentUADTF.initialize(argOIs);
	}

}
