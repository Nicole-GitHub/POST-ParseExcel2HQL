package gss.ETLCode.bin;

import java.util.Map;

public class BEFORE_C03_ExportDate2File {
	
	public static String getHQL(String partition, Map<String, String> mapProp, String tableName ) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:GetCol=YMDS;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- Main\n"
				+ "select ${hivevar:GetCol} AS LINE \n"
				+ "from ${hivevar:RUN_NOW} \n"
				+ "where trim(tablenm) = '${hivevar:Run_TableName}'\n"
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
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C01_Run.hql\n"
				+ "declare SRC2_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C02_Check.hql\n"
				+ "declare SRC3_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C03_ExportDate2File.hql\n"
				+ "\n"
				+ "## DES_PATH: destination uri.\n"
				+ "declare path=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/\n"
				+ "declare DES1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YS.txt\n"
				+ "declare DES2_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YE.txt\n"
				+ "declare DES3_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMS.txt\n"
				+ "declare DES4_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YME.txt\n"
				+ "declare DES5_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDS.txt\n"
				+ "declare DES6_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDE.txt\n"
				+ "\n"
				+ "declare GetCol1=SUBSTRING\\(YMDS,1,4\\)\n"
				+ "declare GetCol2=SUBSTRING\\(YMDE,1,4\\)\n"
				+ "declare GetCol3=SUBSTRING\\(YMDS,1,6\\)\n"
				+ "declare GetCol4=SUBSTRING\\(YMDE,1,6\\)\n"
				+ "declare GetCol5=YMDS\n"
				+ "declare GetCol6=YMDE\n"
				+ "\n"
				+ "declare RUN_TYPE=\n"
				+ "declare DATA_S=\n"
				+ "declare DATA_E=\n"
				+ "\n"
				+ "mkdir -p ${path}\n";

			// 清空文字檔(下面會再產新檔)
			for(int i = 1 ; i <= 6 ; i++) {
				rs += "rm -f ${DES"+i+"_PATH}\n";
			}
			
			rs += "\n"
				+ "# MAIN LOGIC START\n"
				+ "gssSQLConn --user hdfs --logic-file ${SRC1_PATH} --var RUN_TYPE=${RUN_TYPE} --var DATA_S=${DATA_S} --var DATA_E=${DATA_E} --var BATCHID=${BATCHID}\n"
				+ "\n"
				+ "gssSQLConn --user hdfs --logic-file ${SRC2_PATH} --var BATCHID=${BATCHID}\n"
				+ "\n";
		
			// 將BEFORE_C03_ExportDate2File.hql的結果寫出至文字檔
			for(int i = 1 ; i <= 6 ; i++) {
				rs += "gssSQLConn --user hdfs --logic-file ${SRC3_PATH} --var GetCol=${GetCol"+i+"} | \\\n"
					+ "awk '/line/{flag=1; next} /DONE/{flag=0} flag'| \\\n"
					+ "grep '|'| \\\n"
					+ "sed -e 's/^| //g' -e 's/ |$//g' -e 's/ //g' > /${DES"+i+"_PATH}\n"
					+ "\n";
				}
		
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
				+ "declare SRC1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C01_Run.hql\n"
				+ "declare SRC2_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C02_Check.hql\n"
				+ "declare SRC3_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/bin/BEFORE_C03_ExportDate2File.hql\n"
				+ "\n"
				+ "## DES_PATH: destination uri.\n"
				+ "declare path=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/\n"
				+ "declare DES1_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YS.txt\n"
				+ "declare DES2_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YE.txt\n"
				+ "declare DES3_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMS.txt\n"
				+ "declare DES4_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YME.txt\n"
				+ "declare DES5_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDS.txt\n"
				+ "declare DES6_PATH=/opt/gss/pipe-logic-deploy/post/"+tableName+"/RUN_NOW/YMDE.txt\n"
				+ "\n"
				+ "declare GetCol1=SUBSTRING\\(YMDS,1,4\\)\n"
				+ "declare GetCol2=SUBSTRING\\(YMDE,1,4\\)\n"
				+ "declare GetCol3=SUBSTRING\\(YMDS,1,6\\)\n"
				+ "declare GetCol4=SUBSTRING\\(YMDE,1,6\\)\n"
				+ "declare GetCol5=YMDS\n"
				+ "declare GetCol6=YMDE\n"
				+ "\n"
				+ "declare RUN_TYPE=\n"
				+ "declare DATA_S=\n"
				+ "declare DATA_E=\n"
				+ "\n";
		
		return rs;
	}

}
