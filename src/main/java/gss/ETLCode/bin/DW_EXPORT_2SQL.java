package gss.ETLCode.bin;

import java.util.Map;

public class DW_EXPORT_2SQL {
	
	public static String getDBC( Map<String, String> mapProp, String tableName) {
		
		String rs = "TABLE=\""+tableName+"\"\n"
				+ "FUNCTION=\"export\"\n"
				+ "export-dir='/user/hive/dw/post_poc_tmp/"+tableName.toLowerCase()+"'\n"
				+ "input-fields-terminated-by='\\001'\n"
				+ "input-null-string='\\\\N'\n"
				+ "input-null-non-string='\\\\N'\n"
				+ "";

		return rs;
	}
	
	public static String getShell(String type, Map<String, String> mapProp, String tableName) {
		
		String rs = "## FUNCTION: Export data from hive to MSSQL.\n"
				+ "\n"
				+ "########################\n"
				+ "## Parameter liating: ##\n"
				+ "########################\n"
				+ "\n"
				+ "## DB_CONN_CFG: a db connector configuration file.\n"
				+ "declare DB_CONN_CFG=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/"+type+"_EXPORT_2SQL.dbc\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "# MAIN LOGIC START\n"
				+ "\n"
				+ "# gssDBConn: ETL-FRAMEWORK TOOL\n"
				+ "# export data to MSSQL\n"
				+ "\n"
				+ "gssDBConn --var-file ${DB_CONN_CFG}\n"
				+ ""
				+ "";

		return rs;
	}
	
	public static String getShellVAR(String type, Map<String, String> mapProp, String tableName) {
		
		String rs = "## FUNCTION: Export data from hive to MSSQL.\n"
				+ "\n"
				+ "########################\n"
				+ "## Parameter liating: ##\n"
				+ "########################\n"
				+ "\n"
				+ "## DB_CONN_CFG: a db connector configuration file.\n"
				+ "declare DB_CONN_CFG=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/"+type+"_EXPORT_2SQL.dbc\n"
				+ "\n";

		return rs;
	}

}
