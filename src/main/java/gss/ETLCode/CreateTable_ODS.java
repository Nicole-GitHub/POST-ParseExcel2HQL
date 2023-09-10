package gss.ETLCode;

import java.util.Map;

public class CreateTable_ODS {

	public static String getHQL(Map<String, String> mapProp,
			String odsTableName, String rsCreateCols, String rsCreatePartition) {
		
		String rs = "-- HADOOP_ODS \n"
				+ "CREATE TABLE IF NOT EXISTS " + mapProp.get("hadoop.raw.dbname") + "." + odsTableName 
				+ " (\n"
				+ rsCreateCols.substring(0, rsCreateCols.lastIndexOf(",")) 
				+ "\n);\n";
		return rs;
	}


}
