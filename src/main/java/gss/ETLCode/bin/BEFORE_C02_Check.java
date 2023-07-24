package gss.ETLCode.bin;

import java.util.Map;

public class BEFORE_C02_Check {
	
	public static String getHQL(String partition, Map<String, String> mapProp, String tableName ) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:RUN_BEFORE="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_BEFORE;\n"
				+ "set hivevar:Run_Finish="+mapProp.get("hadoop.std.dbname")+".SYS_Run_Finish;\n"
				+ "set hivevar:FUNC_NAME="+tableName+"_Check;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_Check;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- 確認已完成的數量與前置總數量\n"
				+ "drop table ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1} as\n"
				+ "select ab.finish_cnt,c.before_cnt\n"
				+ "from (\n"
				+ "	-- 算出RUN_BEFORE對應的轉檔區間後再與Run_Finish比對已完成的數量\n"
				+ "	select count(1) finish_cnt\n"
				+ "	from (\n"
				+ "		-- 算出RUN_BEFORE對應的轉檔區間\n"
				+ "		select bef.Source_tablenm , bef.Target_tablenm,\n"
				+ "		  case bef.Data_Unit \n"
				+ "		  when 'M' then date_format(trunc(add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(run.ymds, 'yyyyMMdd')), bef.DATA_S), 'MM'),'yyyyMMdd')\n"
				+ "		  when 'D' then date_format(date_add(FROM_UNIXTIME(UNIX_TIMESTAMP(run.ymds, 'yyyyMMdd')), bef.DATA_S),'yyyyMMdd')\n"
				+ "		  else '00000000' end as ymds,\n"
				+ "		  case bef.Data_Unit \n"
				+ "		  when 'M' then date_format(last_day(trunc(add_months(FROM_UNIXTIME(UNIX_TIMESTAMP(run.ymds, 'yyyyMMdd')), bef.DATA_E), 'MM')),'yyyyMMdd')\n"
				+ "		  when 'D' then date_format(date_add(FROM_UNIXTIME(UNIX_TIMESTAMP(run.ymds, 'yyyyMMdd')), bef.DATA_E),'yyyyMMdd')\n"
				+ "		  else '00000000' end as ymde\n"
				+ "		from ${hivevar:RUN_BEFORE} bef\n"
				+ "			, (select ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') run\n"
				+ "		where trim(bef.Target_tablenm) = '${hivevar:Run_TableName}'\n"
				+ "	) a\n"
				+ "	join ${hivevar:Run_Finish} b \n"
				+ "		on a.Source_tablenm = b.tablenm and a.ymds = b.ymds and a.ymde = b.ymde\n"
				+ ") ab\n"
				+ ", (select count(1) before_cnt from ${hivevar:RUN_BEFORE} where trim(Target_tablenm) = '${hivevar:Run_TableName}') c \n"
				+ ";\n"
				+ "\n"
				+ "-- verification \n"
				+ "-- RUN_BEFORE 裡前置作業所需的table是否皆已在 Run_Finish 內\n"
				+ "-- 0 => success; 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select \n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case when before_cnt = 0 then 0 \n"
				+ "	   when finish_cnt = before_cnt then 0 \n"
				+ "	   else 1 end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n";
		
		return rs;
	}
	
	public static String getVAR(String partition, Map<String, String> mapProp, String tableName ) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:RUN_BEFORE="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_BEFORE;\n"
				+ "set hivevar:Run_Finish="+mapProp.get("hadoop.std.dbname")+".SYS_Run_Finish;\n"
				+ "set hivevar:FUNC_NAME="+tableName+"_Check;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_Check;\n"
				+ "-----------------------------------------------------------------\n\n";
		
		return rs;
	}

}
