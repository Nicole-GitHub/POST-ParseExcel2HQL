package gss.ETLCode.bin;

import java.util.Map;

public class ODS_C02_GetDataFileName {

	public static String getHQL(Map<String, String> mapProp, String tableName, String odsTableName) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_C02="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_files;\n"
				+ "set hivevar:DES1_C02="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_files_name;\n"
				+ "set hivevar:FUNC_NAME="+odsTableName+"_GetFileName;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+odsTableName+"_021;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- Main logic\n"
				+ "-- get file name that is needed processing\n"
				+ "drop table if exists ${hivevar:DES1_C02};\n"
				+ "create table if not exists ${hivevar:DES1_C02}\n"
				+ "as\n"
				+ "select \n"
				+ "   row_number() over() as ROW_SEQ,\n"
				+ "   INPUT__FILE__NAME as file_name,\n"
				+ "   * \n"
				+ "from ${hivevar:SRC1_C02}\n"
				+ "where \n"
				+ "   batchid = '${hivevar:BATCHID}'\n"
				+ ";\n"
				+ "\n"
				+ "\n"
				+ "-- verification\n"
				+ "-- first getting the row count from running the load_data_path function\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select\n"
				+ "count(1) as row_count\n"
				+ "from ${hivevar:DES1_C02}\n"
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
				+ "      when row_count > 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp()   as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n"
				+ "\n"
				+ "";
		
		return rs;
	}

	public static String getVAR(Map<String, String> mapProp, String tableName, String odsTableName) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_C02="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_files;\n"
				+ "set hivevar:DES1_C02="+mapProp.get("hadoop.meta.dbname")+"."+odsTableName+"_files_name;\n"
				+ "set hivevar:FUNC_NAME="+odsTableName+"_GetFileName;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+odsTableName+"_021;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
		
		return rs;
	}

}
