package gss.ETLCode.bin;

import java.util.Map;

public class EXPORTFILE {

	public static String getHQL(Map<String, String> mapProp, String colLogicStr, String colENameStr,
			String tableName) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:SRC1_EXPORTFILE="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+";\n"
				+ "set hivevar:TMP1=TMP_DATA_SET_"+tableName+";\n"
				+ "set hivevar:DES1_EXPORTFILE="+mapProp.get("hadoop.raw.dbname")+".OUTPUT_"+tableName+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- Main 組定長\n"
				+ "DROP TABLE IF EXISTS ${hivevar:TMP1};\n"
				+ "CREATE TABLE IF NOT EXISTS ${hivevar:TMP1}\n"
				+ "AS\n"
				+ "select \n" + colLogicStr + "\n"
				+ "FROM ${hivevar:SRC1_EXPORTFILE}\n"
				+ ";\n"
				+ "\n"
				+ "-- 合併成單一欄位\n"
				+ "DROP TABLE IF EXISTS ${hivevar:DES1_EXPORTFILE};\n"
				+ "CREATE TABLE IF NOT EXISTS ${hivevar:DES1_EXPORTFILE}\n"
				+ "AS\n"
				+ "SELECT \n"
				+ "  row_number() over() AS RN,\n"
				+ "  concat(" + colENameStr + "\n"
				+ "    ) AS LINE\n"
				+ "FROM ${hivevar:TMP1}\n"
				+ ";\n"
				+ " \n"
				+ "-- select出最終資料提供所需內容\n"
				+ "SELECT line\n"
				+ "FROM ${hivevar:DES1_EXPORTFILE}\n"
				+ "ORDER BY RN\n"
				+ ";\n";
		
		return rs;
	}

	public static String getShell(String tableName) {
		String tableNameLast = tableName.substring(2);
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
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/EXPORTFILE.hql\n"
				+ "\n"
				+ "\n"
				+ "## DES_PATH: destination uri.\n"
				+ "declare DES1_PATH=/home/post1/DW_WORK/upload/"+tableName+"/tmp/"+tableNameLast+"_$(date +\"%Y%m%d\").txt\n"
				+ "declare DES2_PATH=/home/post1/DW_WORK/upload/"+tableName+"/temp/"+tableNameLast+"_$(date +\"%Y%m%d\").txt\n"
				+ "\n"
				+ "## LOGIC_NAME: logic name.\n"
				+ "declare LOGIC_NAME="+tableName+"\n"
				+ "\n"
				+ "##  FUNC_NAME: main function name.\n"
				+ "declare FUNC_NAME=EXPORTFILE\n"
				+ "\n"
				+ "## KEY_NAME: key_name of msg in the result table.\n"
				+ "declare KEY_NAME=return_code\n"
				+ "\n"
				+ "## TMP1: a table holds verification stats.\n"
				+ "declare TMP1=tmp_tab01_verification_stat\n"
				+ "\n"
				+ "mkdir -p ${HOME}${U_PATH}"+tableName+"/\n"
				+ "mkdir -p ${HOME}${U_PATH}"+tableName+"/tmp/\n"
				+ "mkdir -p ${HOME}${U_PATH}"+tableName+"/temp/\n"
				+ "mkdir -p ${HOME}${U_PATH}"+tableName+"/log/\n"
				+ "\n"
				+ "# MAIN LOGIC START\n"
				+ "\n"
				+ "gssSQLConn --user hdfs --logic-file ${SRC1_PATH} --var BATCHID=${BATCHID} > /${DES1_PATH}\n"
				+ "cat ${DES1_PATH} | awk '/             line             /{flag=1; next} /DONE/{flag=0} flag'| \\\n"
				+ "grep '|'| \\\n"
				+ "sed -e 's/^| //g' -e 's/ |$//g' > /${DES2_PATH}\n"
				+ "\n"
				+ "echo \""+tableName+" start:\" `date` >> ${LOG}\n"
				+ "sh ${PUT_SHELL} -T "+tableName+" -R 0 -F 1 -D ${BATCHID:0:8} -z >> ${LOG}\n"
				+ "echo \""+tableName+" end:\" `date` >> ${LOG}\n";
		
		return rs;
	}

	public static String getShellVAR(String tableName) {
		String tableNameLast = tableName.substring(2);
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
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/EXPORTFILE.hql\n"
				+ "\n"
				+ "\n"
				+ "## DES_PATH: destination uri.\n"
				+ "declare DES1_PATH=/home/post1/DW_WORK/upload/"+tableName+"/tmp/"+tableNameLast+"_$(date +\"%Y%m%d\").txt\n"
				+ "declare DES2_PATH=/home/post1/DW_WORK/upload/"+tableName+"/temp/"+tableNameLast+"_$(date +\"%Y%m%d\").txt\n"
				+ "\n"
				+ "## LOGIC_NAME: logic name.\n"
				+ "declare LOGIC_NAME="+tableName+"\n"
				+ "\n"
				+ "##  FUNC_NAME: main function name.\n"
				+ "declare FUNC_NAME=EXPORTFILE\n"
				+ "\n"
				+ "## KEY_NAME: key_name of msg in the result table.\n"
				+ "declare KEY_NAME=return_code\n"
				+ "\n"
				+ "## TMP1: a table holds verification stats.\n"
				+ "declare TMP1=tmp_tab01_verification_stat\n";
		
		return rs;
	}

}
