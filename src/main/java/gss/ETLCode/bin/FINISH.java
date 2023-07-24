package gss.ETLCode.bin;

import java.util.Map;

public class FINISH {

	public static String getHQL(Map<String, String> mapProp, String tableName) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:Run_Finish="+mapProp.get("hadoop.std.dbname")+".SYS_Run_Finish;\n"
				+ "set hivevar:FUNC_NAME1="+tableName+"_Finish;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_finish;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1} as\n"
				+ "select *\n"
				+ "from ${hivevar:RUN_NOW} \n"
				+ "where trim(tablenm) = '${hivevar:Run_TableName}'\n"
				+ ";\n"
				+ "\n"
				+ "-- 將轉檔完成的資訊寫入Run_Finish\n"
				+ "INSERT OVERWRITE TABLE ${hivevar:Run_Finish}\n"
				+ "PARTITION(TABLENM,YMDS,YMDE) \n"
				+ "select current_timestamp() as CREATEDDATE, TABLENM, YMDS, YMDE\n"
				+ "from ${hivevar:TMP1} \n"
				+ ";\n"
				+ "\n"
				+ "-- verification 轉檔完成的資訊是否進入Run_Finish\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select \n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME1}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case when count(1) > 0 then 0 else 1 end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:Run_Finish} a join ${hivevar:TMP1} b \n"
				+ "on a.tablenm = b.tablenm and a.ymds = b.ymds and a.ymde = b.ymde\n"
				+ ";\n"
				+ "";
		
		return rs;
	}

	public static String getVAR(Map<String, String> mapProp, String tableName) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:Run_Finish="+mapProp.get("hadoop.std.dbname")+".SYS_Run_Finish;\n"
				+ "set hivevar:FUNC_NAME1="+tableName+"_Finish;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_finish;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
		
		return rs;
	}

}
