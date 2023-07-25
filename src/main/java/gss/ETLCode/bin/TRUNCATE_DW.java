package gss.ETLCode.bin;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class TRUNCATE_DW {

	public static String getHQL(String partition, Map<String, String> mapProp, String tableName, String type) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:ACT_YM=202212;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:DES1_Truncate"+type+"="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "set hivevar:FUNC_NAME="+tableName+"_Truncate"+type+";\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_Truncate"+type+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "\n";
			rs += !StringUtils.isBlank(partition) ? 
				"-- 有Partiton\n"
				+ "ALTER TABLE ${hivevar:DES1_Truncate"+type+"} DROP IF EXISTS PARTITION("+partition+"='${hivevar:ACT_YM}');\n"
				: "-- 無Partiton\n"
				+ "TRUNCATE TABLE ${hivevar:DES1_Truncate"+type+"};\n" ;
			rs += "\n"
				+ "-- verification 總筆數\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) as row_count\n"
				+ "from ${hivevar:DES1_Truncate"+type+"} a \n";
			rs += !StringUtils.isBlank(partition) ? 
				 "   join (select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') b\n"
				+ "      on a."+partition+" = b.ymds" : "";
			rs += ";\n\n"
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

	public static String getShell(String tableName, String type) {
		
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
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/TRUNCATE_"+type+".hql\n"
				+ "\n"
				+ "declare ACT_YM=`cat /opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMS.txt`\n"
				+ "\n"
				+ "# MAIN LOGIC START\n"
				+ "gssSQLConn --user hdfs --logic-file ${SRC1_PATH} --var BATCHID=${BATCHID} --var ACT_YM=${ACT_YM}\n";
		
		return rs;
	}

	public static String getShellVAR(String tableName, String type) {
		
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
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/TRUNCATE_"+type+".hql\n"
				+ "\n"
				+ "declare ACT_YM=`cat /opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMS.txt`\n"
				+ "\n";
		
		return rs;
	}

}
