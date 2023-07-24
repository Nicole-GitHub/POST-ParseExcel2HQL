package gss.ETLCode;

import org.apache.commons.lang3.StringUtils;

public class CreateTable_MSSQL {

	public static String getSQL(String pkStr, String rsMSCols, String mssqlTableName) {

		String rs = "-- MSSQL \n"
				+ "CREATE TABLE " + mssqlTableName
				+ " (\n";
		
		rs += StringUtils.isBlank(pkStr) 
			? rsMSCols.substring(0, rsMSCols.lastIndexOf(","))
			: rsMSCols + "\tPRIMARY KEY (" + pkStr.substring(0, pkStr.length() - 1) + ")";
		
		rs += "\n);\n";

		return rs;
	}


}
