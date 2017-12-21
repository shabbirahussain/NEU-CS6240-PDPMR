package org.neu.pdpmr.tasks.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVParser;

import java.io.*;
import java.util.*;

/**
 * Parses the given file into field format specified in the FieldMapping.csv
 * fetched from the packaged resources.
 * @author Navya
 */
public class CSVFlightDataParser {
	private static final String FIELD_INFO_FILE = "resources/packaged/FieldMappings.csv";
	/**
	 * Stores field information for every given field.
	 * @author shabir.ahussain
	 */
	private static class FieldInfo{
		String fieldName;
		String dataType;

		public FieldInfo(String fieldName,
                         String dataType) {
			this.fieldName = fieldName;
			this.dataType = dataType;
		}
	}
	private List<FieldInfo> fieldInfo;

	/**
	 * Loads list of fields from resources.
	 * @throws IOException if resource cannot be loaded.
	 * @throws ArrayIndexOutOfBoundsException is FieldMappings file is misconfigured.
	 */
	public CSVFlightDataParser()
			throws IOException, ArrayIndexOutOfBoundsException {
		this.fieldInfo = new ArrayList<>();

		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(FIELD_INFO_FILE);
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		String line;
		while ((line = br.readLine()) != null){
			String tokens[] = line.split(",");
			fieldInfo.add(new FieldInfo(tokens[0], tokens[1]));
		}
	}

	/**
	 * Parses fields and convert it to data type specified in the config file based on position in line.
	 * @param line is the line of string to parse.
	 * @return A map of column names and parsed values as Object. It is a heterogeneous map.
	 * @throws Exception When there is a parsing error of datatype.
	 */
	public Map<String, Object> parseLine(String line) throws Exception {
		CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT);

		Map<String, Object> res = new HashMap<>();
		for (CSVRecord record : parser) {
		    if(record.size() != fieldInfo.size()) break;
			Iterator itr = record.iterator();

			for (FieldInfo fi : fieldInfo){
				res.put(fi.fieldName,
						parseObject(itr.next().toString(), fi.dataType));
			}
			break; // Process just one line out of multiline text for now.
		}
		return res;
	}

	/**
	 * Tries to cast the given string to the datatype in arguments.
	 * @param s is the string to parse.
	 * @param className is the string java.lang classname to parse.
	 * @return Object that has been successfully parsed and type casted.
	 * @throws Exception When there is a parsing error of datatype.
	 */
    private static Object parseObject(String s, String className)
            throws Exception {
		switch (className) {
			case "Integer": return Integer.parseInt(s);
			case "Boolean": return Boolean.getBoolean(s);
			case "Float":   return Float.parseFloat(s);
			case "Double":  return Double.parseDouble(s);
			case "Long":    return Long.parseLong(s);
			default: return s.trim();
		}
	}
}

