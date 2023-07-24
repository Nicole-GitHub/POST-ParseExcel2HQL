package gss.ETLCode;

import java.util.Map;

public class CreateTable_ODS {

	public static String getHQL(String[] partitionList, Map<String, String> mapProp,
			String odsTableName, String rsCreateCols, String rsCreatePartition) {
		
		String rs = "-- HADOOP_ODS \n"
				+ "CREATE TABLE IF NOT EXISTS " + mapProp.get("hadoop.raw.dbname") + "." + odsTableName + " (\n"
				+ rsCreateCols.substring(0, rsCreateCols.lastIndexOf(",")) + "\n)\n";
		rs += partitionList[0].length() > 0
				? "PARTITIONED BY(" + rsCreatePartition.substring(0, rsCreatePartition.lastIndexOf(",")) + ")"
				: "";
		rs += ";\n";
		
		return rs;
	}


}
