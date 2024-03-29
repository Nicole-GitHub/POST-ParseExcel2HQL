package gss.ETLCode;

import org.apache.commons.lang3.StringUtils;

public class CreateTable_MSSQL {

	public static String getSQL(String pkStr, String rsMSCols, String mssqlTableName) {

		String rs = "-- MSSQL \n"
				+ "CREATE TABLE " + mssqlTableName
				+ " (\n";
		
		mssqlTableName = mssqlTableName.substring(mssqlTableName.lastIndexOf(".")+1);
		rs += StringUtils.isBlank(pkStr) 
			? rsMSCols.substring(0, rsMSCols.lastIndexOf(","))
			: rsMSCols + "\tCONSTRAINT u_"+ mssqlTableName +"_Id UNIQUE (" + pkStr.substring(0, pkStr.length() - 1) + ")";
		rs += "\n);\n";

		return rs;
	}


}
