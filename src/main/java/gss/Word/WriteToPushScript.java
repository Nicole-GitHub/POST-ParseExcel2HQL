package gss.Word;

import java.util.List;
import java.util.Map;

import gss.Tools.FileTools;
import gss.Tools.Tools;

public class WriteToPushScript {
	private static final String className = WriteToPushScript.class.getName();

	public static void run(String outputPath, List<Map<String, String>> controlSheetList, Map<String, String> mapProp) throws Exception {
		
		try {

			String targetTableEName = "", 
				tdTargetTableEName = "",
				sourceTableENameArr = "",
				mkdirDownloadFolder = "", 
//				mkdirHadoopFolder = "", 
//				chmod = "", 
				zip = "", 
				createRSLT = "", 
				dataTransferInterval = "", 
				runInfo = "", 
//				insertRunInfo = "", 
				insertRunInfo_MSSQL = "",
				yyyy = "", 
				yyyyMM = "", 
				yyyyMMDD = "", 
				yyyy1 = "", 
				yyyyMM1 = "", 
				yyyyMM2 = "", 
				yyyyMMDD1 = "", 
				runNowS = "", 
				runNowE = "", 
//				insertRunNow = "", 
//				runType = "", 
//				pipeShell = "",
				selectTDScript = "",
				selectHPScript = "",
				filePath = "",
				gssSQLConn = "";
			
			for(Map<String, String> controlSheetMap : controlSheetList) {

				targetTableEName = controlSheetMap.get("targetTableEName");
				tdTargetTableEName = controlSheetMap.get("tdTargetTableEName");
				sourceTableENameArr = controlSheetMap.get("sourceTableENameArr");

				if("1".equals(mapProp.get("runType"))){
					// mkdir Download Folder
					mkdirDownloadFolder += "mkdir /home/post1/DW_WORK/download/" + targetTableEName + "\n"
							+"mkdir /home/post1/DW_WORK/download/" + targetTableEName + "/BACKUP\n"
							+"mkdir /home/post1/DW_WORK/download/" + targetTableEName + "/DONE\n"
							+"mkdir /home/post1/DW_WORK/download/" + targetTableEName + "/WORK\n\n";
					
//					// mkdir Hadoop Folder
//					mkdirHadoopFolder += "gssShell fs -mkdir /user/post1/Upload/tmp/" + targetTableEName + "/\n";
					
					// zip
					for(String sourceTableEName : sourceTableENameArr.split(",")) {
						String sourceTableENameNoExt = sourceTableEName.indexOf(".") > 0
								? sourceTableEName.substring(0, sourceTableEName.lastIndexOf("."))
								: sourceTableEName;
						zip += sourceTableEName.startsWith("T_") ? "" 
								: "zip -jmqP POST@23931261 /home/post1/DW_WORK/download/" + sourceTableENameNoExt + ".zip "
									+"/home/post1/DW_WORK/download/"+ sourceTableEName + "\n";
					}
				}

//				// chmod
//				chmod += "cd /opt/gss/pipe-logic-deploy/post/" + targetTableEName + "\n"
//						+"chmod 777 main\n"
//						+"cp flow.def_noExport.txt flow.def\n\n";
				
				// Result Table
				createRSLT += "create table post1_post_poc_tmp." + targetTableEName + "_result "
						+ "like post1_post_poc_tmp.T_CMMW_FSMTR_C_TEMP_result; \n"; 
				
				// SYS_RUN_INFO
				dataTransferInterval = controlSheetMap.get("dataTransferInterval").toUpperCase();
				runInfo = "X".equals(dataTransferInterval) ? "'A',0,0"
						: "系統年".equals(dataTransferInterval) ? "'Y',0,0"
							: "系統年-1".equals(dataTransferInterval) ? "'Y',-1,-1"
								: "系統月".equals(dataTransferInterval) ? "'M',0,0"
									: "系統月-1".equals(dataTransferInterval) ? "'M',-1,-1"
										: "系統月-2".equals(dataTransferInterval) ? "'M',-2,-2"
											: "系統日".equals(dataTransferInterval) ? "'D',0,0"
												: "系統日-1".equals(dataTransferInterval) ? "'D',-1,-1"
													: "";
//				// HADOOP
//				insertRunInfo += "INSERT OVERWRITE TABLE post1_post_poc_std.SYS_RUN_INFO PARTITION(TABLENM) "
//						+"SELECT "+runInfo+", CURRENT_TIMESTAMP, '" + targetTableEName + "';\n";
				
				// MSSQL
				insertRunInfo_MSSQL += "INSERT INTO SYS_RUN_INFO "
						+"SELECT '" + targetTableEName + "',"+runInfo+", GETDATE() ;\n";
				
				// SYS_RUN_NOW
				yyyy = Tools.getNOW("yyyy");
				yyyyMM = Tools.getNOW("yyyyMM");
				yyyyMMDD = Tools.getNOW("yyyyMMdd");
				yyyy1 = String.valueOf(Integer.parseInt(Tools.getNOW("yyyy"))-1);
				yyyyMM1 = String.valueOf(Integer.parseInt(Tools.getNOW("yyyyMM"))-1);
				yyyyMM2 = String.valueOf(Integer.parseInt(Tools.getNOW("yyyyMM"))-2);
				yyyyMMDD1 = String.valueOf(Integer.parseInt(Tools.getNOW("yyyyMMdd"))-1);
				runNowS = "X".equals(dataTransferInterval) ? "00000000"
						: "系統年".equals(dataTransferInterval) ? yyyy+"0101"
							: "系統年-1".equals(dataTransferInterval) ? yyyy1 +"0101"
								: "系統月".equals(dataTransferInterval) ? yyyyMM+"01"
									: "系統月-1".equals(dataTransferInterval) ? yyyyMM1+"01"
										: "系統月-2".equals(dataTransferInterval) ? yyyyMM2+"01"
											: "系統日".equals(dataTransferInterval) ? yyyyMMDD
												: "系統日-1".equals(dataTransferInterval) ? yyyyMMDD1
													: "";

				runNowE = "X".equals(dataTransferInterval) ? "00000000"
						: "系統年".equals(dataTransferInterval) ? yyyy+"1231"
							: "系統年-1".equals(dataTransferInterval) ? yyyy1+"1231"
								: "系統月".equals(dataTransferInterval) ? yyyyMM+"31"
									: "系統月-1".equals(dataTransferInterval) ? yyyyMM1+"31"
										: "系統月-2".equals(dataTransferInterval) ? yyyyMM2+"31"
											: "系統日".equals(dataTransferInterval) ? yyyyMMDD
												: "系統日-1".equals(dataTransferInterval) ? yyyyMMDD
													: "";
				
//				insertRunNow += "INSERT OVERWRITE TABLE post1_post_poc_std.SYS_RUN_NOW PARTITION(TABLENM) "
//						+"SELECT '" + runNowS + "', '" + runNowE + "', CURRENT_TIMESTAMP, '" + targetTableEName + "';\n";
				
				// pipeShell
//				runType = "X".equals(dataTransferInterval) ? "--var RUN_TYPE=A"
//							: "--var RUN_TYPE=M --var DATA_S=" + runNowS + " --var DATA_E=" + runNowE;
//				pipeShell += "pipe-shell --logic-uri /opt/gss/pipe-logic-deploy/post/" + targetTableEName + " "
//						+ runType + " --is-staged --is-bypass-pre-check --clear-cache \n";
				
				// Select Source Table Script
				String whereScript = "X".equals(dataTransferInterval) ? ""
						: "where YR || MON between '" + runNowS + "' and '" + runNowE + "'";
				
				// Select Teradata Source Table Script
				selectTDScript += "\nselect * from "+ tdTargetTableEName +" t " + whereScript + " ; \n";
				for(String tdSourceTableEName : controlSheetMap.get("tdSourceTableENameArr").split(",")) {
					selectTDScript += "select * from "+ tdSourceTableEName +" t " + whereScript + " ; \n";
				}
				
				// Select Hadoop Source Table Script (CNT)
				selectHPScript += "\nselect '" + targetTableEName + "', count(1) cnt from post1_post_poc_raw."+ targetTableEName +" t " + whereScript + " \n";
				for(String sourceTableEName : sourceTableENameArr.split(",")) {
					selectHPScript += "union all select '" + sourceTableEName + "', count(1) cnt from post1_post_poc_raw."+ sourceTableEName +" t " + whereScript + " \n";
				}
				selectHPScript += "; \n";
				
				// Truncate Hadoop Source Table Script
				selectHPScript += "\nTRUNCATE TABLE post1_post_poc_raw."+ targetTableEName +" ; \n";
				for(String sourceTableEName : sourceTableENameArr.split(",")) {
					selectHPScript += "TRUNCATE TABLE post1_post_poc_raw."+ sourceTableEName +" ; \n";
				}
				
				// Select Hadoop Source Table Script
				selectHPScript += "\nselect * from post1_post_poc_raw."+ targetTableEName +" t " + whereScript + " ; \n";
				for(String sourceTableEName : sourceTableENameArr.split(",")) {
					selectHPScript += "select * from post1_post_poc_raw."+ sourceTableEName +" t " + whereScript + " ; \n";
				}
				
				// gssSQLConn
				filePath = "/opt/gss/pipe-logic-deploy/post/" + targetTableEName;
				String gssSQLConnHead = "sh /DAG_WORK/scripts/gssSQLConn.sh --logic-file " + filePath;
				gssSQLConn += gssSQLConnHead + "/bin/DM_T01.hql 1> " + filePath + "/log/01.txt 2>&1\n"
						+ gssSQLConnHead + "/bin/DM_T02.hql 1> " + filePath + "/log/02.txt 2>&1\n"
						+ gssSQLConnHead + "/bin/DM_T03.hql 1> " + filePath + "/log/03.txt 2>&1\n"
						+ gssSQLConnHead + "/bin/DM_T04.hql 1> " + filePath + "/log/04.txt 2>&1\n"
						+ gssSQLConnHead + "/bin/DM_T05.hql 1> " + filePath + "/log/05.txt 2>&1\n"
						+ gssSQLConnHead + "/bin/DM_L06_LoadDM.hql 1> " + filePath + "/log/06.txt 2>&1\n\n"; 

			}
			
//			// 收載時才會有前三項
//			String rs =	(
//					!"1".equals(mapProp.get("runType")) ? ""
//						: ("--mkdirDownloadFolder\n" + mkdirDownloadFolder
////						+ "\n--mkdirHadoopFolder\n" + mkdirHadoopFolder
//						+ "\n--zip\n" + zip )
//					)
//					+ "\n--chmod\n" + chmod
//					+ "\n--insertRunInfo\n" + insertRunInfo
//					+ "\n--createRSLT\n" + createRSLT
//					+ "\n--insertRunNow\n" + insertRunNow
//					+ "\n--pipeShell\n" + pipeShell
//					+ "\n--selectTDScript\n" + selectTDScript
//					+ "\n--selectHPScript\n" + selectHPScript
//					+ "\n--gssSQLConn\n" + gssSQLConn ;
			
//			FileTools.createFileNotAppend(outputPath, "PushScript", "sql", rs);
			
			// 收載時才會有前三項
			String rs_airflow =	(
					!"1".equals(mapProp.get("runType")) ? ""
						: ("--mkdirDownloadFolder\n" + mkdirDownloadFolder
//						+ "\n--mkdirHadoopFolder\n" + mkdirHadoopFolder
						+ "\n--zip\n" + zip )
					)
//					+ "\n--chmod\n" + chmod
//					+ "\n--insertRunInfo\n" + insertRunInfo
					+ "\n--insertRunInfo_MSSQL\n" + insertRunInfo_MSSQL
					+ "\n--createRSLT\n" + createRSLT
//					+ "\n--insertRunNow\n" + insertRunNow
//					+ "\n--pipeShell\n" + pipeShell
					+ "\n--selectTDScript\n" + selectTDScript
					+ "\n--selectHPScript\n" + selectHPScript
					+ "\n--gssSQLConn\n" + gssSQLConn 
					;
			
			FileTools.createFileAppend(outputPath, "PushScript_airflow", "sql", rs_airflow);
			
			

		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
}
