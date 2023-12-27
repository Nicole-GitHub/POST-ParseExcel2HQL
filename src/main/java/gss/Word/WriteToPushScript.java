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
				sourceTableEName = "",
				mkdirDownloadFolder = "", 
				mkdirHadoopFolder = "", 
				chmod = "", 
				zip = "", 
				createRSLT = "", 
				dataTransferInterval = "", 
				runInfo = "", 
				insertRunInfo = "", 
				yyyy = "", 
				yyyyMM = "", 
				yyyyMMDD = "", 
				yyyy1 = "", 
				yyyyMM1 = "", 
				yyyyMMDD1 = "", 
				runNowS = "", 
				runNowE = "", 
				insertRunNow = "", 
				runType = "", 
				pipeShell = "",
				selectSourceScript = "",
				filePath = "",
				gssSQLConn = "";
			
			for(Map<String, String> controlSheetMap : controlSheetList) {

				targetTableEName = controlSheetMap.get("targetTableEName");
				sourceTableEName = controlSheetMap.get("sourceTableEName");

				if("1".equals(mapProp.get("runType"))){
					// mkdir Download Folder
					mkdirDownloadFolder += "mkdir /home/post1/DW_WORK/download/" + targetTableEName + "\n"
							+"mkdir /home/post1/DW_WORK/download/" + targetTableEName + "/BACKUP\n"
							+"mkdir /home/post1/DW_WORK/download/" + targetTableEName + "/DONE\n"
							+"mkdir /home/post1/DW_WORK/download/" + targetTableEName + "/WORK\n\n";
					
					// mkdir Hadoop Folder
					mkdirHadoopFolder += "gssShell fs -mkdir /user/post1/Upload/tmp/" + targetTableEName + "/\n";
					
					// zip
					zip += sourceTableEName.startsWith("T_") ? "" 
							: "zip -jmqP POST@23931261 /home/post1/DW_WORK/download/"+controlSheetMap.get("sourceTableENameNoExt")+".zip "
								+"/home/post1/DW_WORK/download/"+ sourceTableEName + "\n";
				}

				// chmod
				chmod += "cd /opt/gss/pipe-logic-deploy/post/" + targetTableEName + "\n"
						+"chmod 777 main\n"
						+"cp flow.def_noExport.txt flow.def\n\n";
				
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
												: "系統日".equals(dataTransferInterval) ? "'D',0,0"
													: "系統日-1".equals(dataTransferInterval) ? "'D',-1,-1"
														: "";
				
				insertRunInfo += "INSERT OVERWRITE TABLE post1_post_poc_std.SYS_RUN_INFO PARTITION(TABLENM) "
						+"SELECT "+runInfo+", CURRENT_TIMESTAMP, '" + targetTableEName + "';\n";
				
				// SYS_RUN_NOW
				yyyy = Tools.getNOW("yyyy");
				yyyyMM = Tools.getNOW("yyyyMM");
				yyyyMMDD = Tools.getNOW("yyyyMMdd");
				yyyy1 = String.valueOf(Integer.parseInt(Tools.getNOW("yyyy"))-1);
				yyyyMM1 = String.valueOf(Integer.parseInt(Tools.getNOW("yyyyMM"))-1);
				yyyyMMDD1 = String.valueOf(Integer.parseInt(Tools.getNOW("yyyyMMdd"))-1);
				runNowS = "X".equals(dataTransferInterval) ? "00000000"
						: "系統年".equals(dataTransferInterval) ? yyyy+"0101"
							: "系統年-1".equals(dataTransferInterval) ? yyyy1 +"0101"
								: "系統月".equals(dataTransferInterval) ? yyyyMM+"01"
									: "系統月-1".equals(dataTransferInterval) ? yyyyMM1+"01"
										: "系統日".equals(dataTransferInterval) ? yyyyMMDD
											: "系統日-1".equals(dataTransferInterval) ? yyyyMMDD1
												: "";

				runNowE = "X".equals(dataTransferInterval) ? "00000000"
						: "系統年".equals(dataTransferInterval) ? yyyy+"1231"
							: "系統年-1".equals(dataTransferInterval) ? yyyy1+"1231"
								: "系統月".equals(dataTransferInterval) ? yyyyMM+"31"
									: "系統月-1".equals(dataTransferInterval) ? yyyyMM1+"31"
										: "系統日".equals(dataTransferInterval) ? yyyyMMDD
											: "系統日-1".equals(dataTransferInterval) ? yyyyMMDD
												: "";
				
				insertRunNow += "INSERT OVERWRITE TABLE post1_post_poc_std.SYS_RUN_NOW PARTITION(TABLENM) "
						+"SELECT '" + runNowS + "', '" + runNowE + "', CURRENT_TIMESTAMP, '" + targetTableEName + "';\n";
				
				// pipeShell
				runType = "X".equals(dataTransferInterval) ? "--var RUN_TYPE=A"
							: "--var RUN_TYPE=M --var DATA_S=" + runNowS + " --var DATA_E=" + runNowE;
				pipeShell += "pipe-shell --logic-uri /opt/gss/pipe-logic-deploy/post/" + targetTableEName + " "
						+ runType + " --is-staged --is-bypass-pre-check --clear-cache \n";
				
				// Select Source Table Script
				selectSourceScript = "select * from post1_post_poc_raw."+ sourceTableEName +"  t ; \n";
				
				// gssSQLConn
				filePath = "/opt/gss/pipe-logic-deploy/post/";
				gssSQLConn = "gssSQLConn --user hdfs --logic-file " + filePath + targetTableEName + "/bin/DM_T01.hql 1> " + filePath + targetTableEName + "/log/01.txt 2>&1\n"
						+ "gssSQLConn --user hdfs --logic-file " + filePath + targetTableEName + "/bin/DM_L03_LoadDM.hql 1> " + filePath + targetTableEName + "/log/03.txt 2>&1\n\n"; 

			}
			
			// 收載時才會有前三項
			String rs =	(
					!"1".equals(mapProp.get("runType")) ? ""
						: ("--mkdirDownloadFolder\n" + mkdirDownloadFolder
						+ "\n--mkdirHadoopFolder\n" + mkdirHadoopFolder
						+ "\n--zip\n" + zip )
					)
					+ "\n--chmod\n" + chmod
					+ "\n--createRSLT\n" + createRSLT
					+ "\n--insertRunInfo\n" + insertRunInfo
					+ "\n--insertRunNow\n" + insertRunNow
					+ "\n--pipeShell\n" + pipeShell
					+ "\n--selectSourceScript\n" + selectSourceScript
					+ "\n--gssSQLConn\n" + gssSQLConn ;
			
			FileTools.createFileNotAppend(outputPath, "PushScript", "sql", rs);

		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
}
