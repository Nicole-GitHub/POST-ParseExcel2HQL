package gss.ETLCode.bin;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class DW_L08_LoadDW_TMP {

	public static String getHQL(String partition, Map<String, String> mapProp, String tableName, String selectStr, String type) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_L08="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "set hivevar:DES1_L08="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+";\n"
				+ "set hivevar:FUNC_NAME="+tableName+"_Load"+type+"_TMP;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_081;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- main\n"
				+ "drop table if exists ${hivevar:DES1_L08};\n"
				+ "\n"
				+ "create table ${hivevar:DES1_L08}\n"
				+ "as\n"
				+ "select \n" + selectStr.substring(0,selectStr.lastIndexOf(",")) + "\n"
				+ "from ${hivevar:SRC1_L08} a\n";
			rs += !StringUtils.isBlank(partition) ? 
				 "	join (select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') b\n"
				+ "	on a." + partition + " = b.ymds\n" : "";
			rs += ";\n"
				+ "\n"
				+ "\n"
				+ "-- verification 總筆數\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) as row_count\n"
				+ "from ${hivevar:DES1_L08}\n"
				+ ";\n"
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
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n";
			
		return rs;
		
	}

	public static String getVAR(Map<String, String> mapProp, String tableName, String type) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_L08="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "set hivevar:DES1_L08="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+";\n"
				+ "set hivevar:FUNC_NAME="+tableName+"_Load"+type+"_TMP;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_081;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
			
		return rs;
		
	}

	public static String getHQL_airflow(String partition, Map<String, String> mapProp, String tableName, String selectStr, String type) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:ACT_YM=202303;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_L08="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "set hivevar:DES1_L08="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+";\n"
				+ "set hivevar:FUNC_NAME="+tableName+"_Load"+type+"_TMP;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_081;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- main\n"
				+ "drop table if exists ${hivevar:DES1_L08};\n"
				+ "\n"
				+ "create table ${hivevar:DES1_L08}\n"
				+ "as\n"
				+ "select \n" + selectStr.substring(0,selectStr.lastIndexOf(",")) + "\n"
				+ "from ${hivevar:SRC1_L08} a\n";
			rs += !StringUtils.isBlank(partition) ? 
				 "where ACT_YM=${hivevar:ACT_YM}\n" : "";
			rs += ";\n"
				+ "\n"
				+ "\n"
				+ "-- verification 總筆數\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) as row_count\n"
				+ "from ${hivevar:DES1_L08}\n"
				+ ";\n"
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
				+ "      when row_count > 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n";
			
		return rs;
		
	}

}
