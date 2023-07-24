package gss.ETLCode;

public class CreateTable_T {

	public static String getHQL(String[] partitionList, String rsCreatePartition, String rsHPCols,
			String hadoopTableName) {

		String rs = "-- HADOOP_T \n" 
				+ "CREATE TABLE IF NOT EXISTS "
				+ hadoopTableName + " (\n" + rsHPCols.substring(0, rsHPCols.lastIndexOf(",")) + "\n)\n";
		rs += partitionList[0].length() > 0
				? "PARTITIONED BY(" + rsCreatePartition.substring(0, rsCreatePartition.lastIndexOf(",")) + ")"
				: "";
		rs += ";\n";

		return rs;
	}
}
