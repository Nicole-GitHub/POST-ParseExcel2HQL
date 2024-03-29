package gss.ETLCode.bin;

import java.util.Map;

public class ODS_C03_GetErrorData {

	public static String getHQL(Map<String, String> mapProp, String tableName, String odsTableName, String finalLen, boolean hasChinese) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:FIX_LENGTH="+finalLen+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_C03="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_files_name;\n"
				+ "set hivevar:DES1_C03="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_ERR;\n"
				+ "set hivevar:FUNC_NAME="+odsTableName+"_GetErrorData;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+odsTableName+"_031;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- Main logic\n"
				+ "--get wrong fix length data\n"
				+ "drop table if exists ${hivevar:DES1_C03};\n"
				+ "\n"
				+ "create table if not exists ${hivevar:DES1_C03}\n"
				+ "as\n"
				
				+ "select *,LENGTH("+(hasChinese ? "ENCODE(LINE,'BIG5')" : "LINE")+") as FIX_LENGTH\n"
				+ "from  ${hivevar:SRC1_C03}\n"
				+ "WHERE LENGTH("+(hasChinese ? "ENCODE(LINE,'BIG5')" : "LINE")+") <> ${hivevar:FIX_LENGTH}\n"
				+ ";\n"
				+ "\n"
				+ "-- verification\n"
				+ "-- first getting the row count from running the load_data_path function\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select\n"
				+ "count(1) as row_count\n"
				+ "from ${hivevar:DES1_C03}\n"
				+ ";\n"
				+ "\n"
				+ "-- verification(2/2)\n"
				+ "-- secod, record the return code\n"
				+ "-- in this case\n"
				+ "-- row_count > 0 return 0\n"
				+ "-- else return 1\n"
				+ "\n"
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
				+ "      when row_count = 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";";
		
		return rs;
	}

	public static String getVAR(Map<String, String> mapProp, String tableName, String odsTableName, String finalLen) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:FIX_LENGTH="+finalLen+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_C03="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_files_name;\n"
				+ "set hivevar:DES1_C03="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_ERR;\n"
				+ "set hivevar:FUNC_NAME="+odsTableName+"_GetErrorData;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+odsTableName+"_031;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
		
		return rs;
	}
	
	public static String getHQL_airflow(Map<String, String> mapProp, String tableName, String odsTableName, String finalLen, boolean hasChinese) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:FIX_LENGTH="+finalLen+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_C03="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_files_name;\n"
				+ "set hivevar:DES1_C03="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_ERR;\n"
				+ "set hivevar:FUNC_NAME="+odsTableName+"_GetErrorData;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+odsTableName+"_031;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- Main logic\n"
				+ "--get wrong fix length data\n"
				+ "drop table if exists ${hivevar:DES1_C03};\n"
				+ "\n"
				+ "create table if not exists ${hivevar:DES1_C03}\n"
				+ "as\n"
				
				+ "select *,LENGTH("+(hasChinese ? "ENCODE(LINE,'BIG5')" : "LINE")+") as FIX_LENGTH\n"
				+ "from  ${hivevar:SRC1_C03}\n"
				+ "WHERE LENGTH("+(hasChinese ? "ENCODE(LINE,'BIG5')" : "LINE")+") <> ${hivevar:FIX_LENGTH}\n"
				+ ";\n"
				+ "\n"
				+ "-- verification\n"
				+ "-- first getting the row count from running the load_data_path function\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select\n"
				+ "count(1) as row_count\n"
				+ "from ${hivevar:DES1_C03}\n"
				+ ";\n"
				+ "\n"
				+ "-- verification(2/2)\n"
				+ "-- secod, record the return code\n"
				+ "-- in this case\n"
				+ "-- row_count > 0 return 0\n"
				+ "-- else return 1\n"
				+ "\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   '${hivevar:FUNC_NAME}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when row_count = 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";";
		
		return rs;
	}

}
