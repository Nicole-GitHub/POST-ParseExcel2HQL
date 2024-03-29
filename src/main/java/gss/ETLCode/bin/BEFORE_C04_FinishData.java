package gss.ETLCode.bin;

import java.util.Map;

public class BEFORE_C04_FinishData {

	public static String getHQL(String partition, Map<String, String> mapProp, String tableName ) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:YMDS=20201201;\n"
				+ "set hivevar:YMDE=20201231;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:Run_Finish="+mapProp.get("hadoop.std.dbname")+".SYS_Run_Finish;\n"
				+ "set hivevar:Run_Finish_His="+mapProp.get("hadoop.std.dbname")+".SYS_Run_Finish_His;\n"
				+ "set hivevar:FUNC_NAME1="+tableName+"_BackupFinishData;\n"
				+ "set hivevar:FUNC_NAME2="+tableName+"_CleanFinishData;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_BackupFinishData;\n"
				+ "set hivevar:TMP2=tmp_"+tableName+"_CheckBackupFinishData;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- 確認目前要跑的資料是否存在在Run_Finish裡\n"
				+ "-- 因以下script會用到兩次，故先放至tmp檔內\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1} as\n"
				+ "select a.tablenm, a.ymds, a.ymde, a.createddate\n"
				+ "from ${hivevar:Run_Finish} a\n"
				+ "join (select tablenm, ymds, ymde from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') run\n"
				+ "	on a.tablenm = run.tablenm and a.ymds = run.ymds and a.ymde = run.ymde\n"
				+ ";\n"
				+ "\n"
				+ "-- 將目前要跑的資料從Run_Finish搬至Run_Finish_His\n"
				+ "insert into ${hivevar:Run_Finish_His}\n"
				+ "select * from ${hivevar:TMP1}\n"
				+ ";\n"
				+ "\n"
				+ "-- 將目前要跑的資料從Run_Finish內移除(不論是否已存在)\n"
				+ "ALTER TABLE ${hivevar:Run_Finish} DROP IF EXISTS PARTITION(TABLENM='${hivevar:Run_TableName}',YMDS='${hivevar:YMDS}',YMDE='${hivevar:YMDE}');\n"
				+ "\n"
				+ "-- 確認是否已搬移至Run_Finish_His裡\n"
				+ "drop table if exists ${hivevar:TMP2};\n"
				+ "create table ${hivevar:TMP2} as\n"
				+ "select count(1) as row_count\n"
				+ "from ${hivevar:TMP1} a\n"
				+ "join ${hivevar:Run_Finish_His} b \n"
				+ "on a.tablenm = b.tablenm and a.YMDS = b.YMDS and a.YMDE = b.YMDE and a.CREATEDDATE = b.CREATEDDATE\n"
				+ ";\n"
				+ "\n"
				+ "-- sum(row_count) = 0，表示此job第一次跑，Run_Finish無值\n"
				+ "-- sum(row_count) = 1，表示此job非第一次跑，Run_Finish有值，但未成功搬至Run_Finish_His\n"
				+ "-- sum(row_count) = 2，表示此job非第一次跑，Run_Finish有值，且已成功搬至Run_Finish_His\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME1}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when sum(row_count) = 1 then 1\n"
				+ "      else 0\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from (\n"
				+ "   select count(1) row_count from ${hivevar:TMP1}\n"
				+ "   union all\n"
				+ "   select row_count from ${hivevar:TMP2}\n"
				+ "   ) a\n"
				+ ";\n"
				+ "\n"
				+ "-- 確認Run_Finish是否已成功DROP PARTITION\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME2}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when count(1) > 0 then 1\n"
				+ "      else 0\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:Run_Finish} a\n"
				+ "join (select tablenm, ymds, ymde from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') run\n"
				+ "   on a.tablenm = run.tablenm and a.ymds = run.ymds and a.ymde = run.ymde\n"
				+ ";\n";
		
		return rs;
	}
	
	public static String getShell(String tableName ) {
		
		String rs = "########################\n"
				+ "## Parameter liating: ##\n"
				+ "########################\n"
				+ "##    BATCHID: batchid, job id. the framework will handle this.\n"
				+ "source ${ENV_STR}\n"
				+ "declare LOG=${HOME}${U_PATH}'"+tableName+"/log/log_'$(date +'%Y%m%d')'.log'\n"
				+ "\n"
				+ "declare BATCHID=20230329000000;\n"
				+ "\n"
				+ "## SRC_PATH: source uri. local directory.\n"
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C04_FinishData.hql\n"
				+ "\n"
				+ "declare YMDS=`cat /opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDS.txt`\n"
				+ "declare YMDE=`cat /opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDE.txt`\n"
				+ "\n"
				+ "# MAIN LOGIC START\n"
				+ "gssSQLConn --user hdfs --logic-file ${SRC1_PATH} --var BATCHID=${BATCHID} --var YMDS=${YMDS} --var YMDE=${YMDE}\n";
		
		return rs;
	}
	
	public static String getShellVAR(String tableName ) {
		
		String rs = "########################\n"
				+ "## Parameter liating: ##\n"
				+ "########################\n"
				+ "##    BATCHID: batchid, job id. the framework will handle this.\n"
				+ "source ${ENV_STR}\n"
				+ "declare LOG=${HOME}${U_PATH}'"+tableName+"/log/log_'$(date +'%Y%m%d')'.log'\n"
				+ "\n"
				+ "declare BATCHID=20230329000000;\n"
				+ "\n"
				+ "## SRC_PATH: source uri. local directory.\n"
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C04_FinishData.hql\n"
				+ "\n"
				+ "declare YMDS=`cat /opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDS.txt`\n"
				+ "declare YMDE=`cat /opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDE.txt`\n"
				+ "\n";
		
		return rs;
	}

}
