package gss.ETLCode.bin;

import java.util.Map;

public class TRUNCATE_ODS {
	
	public static String getHQL(Map<String, String> mapProp, String tableName, String odsTableName) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:DES1_TruncateODS="+mapProp.get("hadoop.raw.dbname")+"."+odsTableName+";\n"
				+ "set hivevar:FUNC_NAME="+odsTableName+"_TruncateODS;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+odsTableName+"_TruncateODS;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "\n"
				+ "TRUNCATE TABLE ${hivevar:DES1_TruncateODS};\n"
				+ "\n"
				+ "\n"
				+ "-- verification 總筆數\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) as row_count\n"
				+ "from ${hivevar:DES1_TruncateODS}\n"
				+ ";\n"
				+ "\n"
				+ "\n"
				+ "-- 確認Truncate後的筆數\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when row_count > 0 then 1\n"
				+ "      else 0\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n";
		
		return rs;
	}
	
	public static String getVAR(Map<String, String> mapProp, String tableName, String odsTableName) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:DES1_TruncateODS="+mapProp.get("hadoop.raw.dbname")+"."+odsTableName+";\n"
				+ "set hivevar:FUNC_NAME="+odsTableName+"_TruncateODS;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+odsTableName+"_TruncateODS;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
		
		return rs;
	}

}
