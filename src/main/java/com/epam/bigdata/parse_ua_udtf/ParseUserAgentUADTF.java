package com.epam.bigdata.parse_ua_udtf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

public class ParseUserAgentUADTF extends GenericUDTF {

	private PrimitiveObjectInspector stringIO;

	@Override
	public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
		List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
		if (inputFields.size() != 1) {
			throw new UDFArgumentException(getClass().getSimpleName() + " takes exactly one argument");
		}
		ObjectInspector udtfInputIO = inputFields.get(0).getFieldObjectInspector();
		if (udtfInputIO.getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) udtfInputIO)
				.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException(getClass().getSimpleName() + " takes a string as a parameter");
		}

		stringIO = (PrimitiveObjectInspector) udtfInputIO;
		List<String> fieldNames = new ArrayList<>(Arrays.asList("device", "os", "browser"));
		List<ObjectInspector> fieldOIs = new ArrayList<>(3);
		for (int i = 0; i < 3; i++) {
			fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		}
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		final String userAgentString = stringIO.getPrimitiveJavaObject(args[0]).toString();
		UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
		Object[] userAgentFields = new Object[] { userAgent.getOperatingSystem().getDeviceType().getName(),
				userAgent.getOperatingSystem().getName(), userAgent.getBrowser().getName() };
		forward(userAgentFields);
	}

	@Override
	public void close() throws HiveException {
	}

}
