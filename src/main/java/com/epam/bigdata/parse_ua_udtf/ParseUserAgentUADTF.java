package com.epam.bigdata.parse_ua_udtf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import eu.bitwalker.useragentutils.UserAgent;

@Description(
	name = "parseUA",
	value = "parseUA(arg1) - parse any user agent (UA) string into separate fields",
	extended = "parseUA(user_agent String) parse any user agent (UA) string into "
			+ "three separate fields: device, os, browser. "
			+ "Example: uatx lateral view parseUA(user_agent) uap as device, os, browser"
		)
public class ParseUserAgentUADTF extends GenericUDTF {

	private PrimitiveObjectInspector stringIO;
	
	/*
	* This method will be called exactly once per instance.
	* It performs initialization logic of this function.
	* It is also responsible for verifying the input types and
	* specifying the output types.
	*/

	@Override
	public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
		List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
		// Check number of arguments.
		if (inputFields.size() != 1) {
			throw new UDFArgumentException(getClass().getSimpleName() + " takes exactly one argument");
		}
		ObjectInspector udtfInputIO = inputFields.get(0).getFieldObjectInspector();
		/*
		* Check that the input ObjectInspector[] array contains a
		* single PrimitiveObjectInspector of the Primitive type,
		* such as String.
		*/
		if (udtfInputIO.getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) udtfInputIO)
				.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(getClass().getSimpleName() + " takes a string as a parameter");
		}
		stringIO = (PrimitiveObjectInspector) udtfInputIO;
		/*
		* Define the expected output for this function, including
		* each alias and types for the aliases.
		*/
		List<String> fieldNames = new ArrayList<>(Arrays.asList("device", "os", "browser"));
		List<ObjectInspector> fieldOIs = new ArrayList<>(3);
		for (int i = 0; i < 3; i++) {
			fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		}
		//Set up the output schema.
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}


	/*
	* This method is called once per input row and generates
	* output. The "forward" method is used (instead of
	* "return") in order to specify the output from the function.
	*/
	@Override
	public void process(Object[] args) throws HiveException {
		/*
		* Convert the object to a primitive type before
		* implementing logic.
		*/ 
		final String userAgentString = stringIO.getPrimitiveJavaObject(args[0]).toString();
		UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
		/*
		* Parse string and extract fields from it.
		*/
		Object[] userAgentFields = new Object[] { userAgent.getOperatingSystem().getDeviceType().getName(),
				userAgent.getOperatingSystem().getName(), userAgent.getBrowser().getName() };
		forward(userAgentFields);
	}

	@Override
	public void close() throws HiveException {
	}

}
