package gss.Write;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import gss.ETLCode.RCPT;
import gss.ETLCode.bin.BACKUP_DW;
import gss.ETLCode.bin.BEFORE_C01_Run;
import gss.ETLCode.bin.DM_L02_LoadDM;
import gss.ETLCode.bin.DW_EXPORT_2SQL;
import gss.ETLCode.bin.DW_L08_LoadDW_TMP;
import gss.ETLCode.bin.ODS_C01_UploadFile;
import gss.ETLCode.bin.ODS_C02_GetDataFileName;
import gss.ETLCode.bin.ODS_C03_GetErrorData;
import gss.ETLCode.bin.ODS_C04_GetData;
import gss.ETLCode.bin.ODS_C05_GetDoneFile;
import gss.ETLCode.bin.Python_Airflow;
import gss.ETLCode.bin.TRUNCATE_DW;
import gss.ETLCode.bin.TRUNCATE_ODS;
import gss.Tools.FileTools;

/**
 * 產出其餘所需檔案
 * @author nicole_tsou
 *
 */
public class WriteToOther {
	private static final String className = WriteToOther.class.getName();

	public static void run(String outputPath, String fileName, List<Map<String, String>> layoutMapList, String finalLen,
			String hasChineseForTable, String txtFileName, Map<String, String> mapProp) throws Exception {
		try {

			outputPath += fileName + "/";
			String outputPathBin = outputPath + "bin/";
			
			// list的最後一筆位置
			Map<String, String> layoutMap = layoutMapList.get(layoutMapList.size() - 1);
			String tableName = layoutMap.get("TableName");
	     	String odsTableName = "ODS" + tableName.substring(1);
	     	String partition = layoutMap.get("Partition");
			String runType = mapProp.get("runType");
			String type = "1".equals(runType) ? "DW" : "DM";
			boolean hasChinese = "Y".equals(hasChineseForTable);
			
			// 將所有欄位名稱合在一起
			String selectStr = "";
			for (Map<String, String> layoutMapfor : layoutMapList) {
				if ("Detail".equals(layoutMapfor.get("MapType"))) {
					selectStr += "\t" + layoutMapfor.get("ColEName") + ",\n";
				}
			}
			
			/******************
			 * pipe-shell
			 ******************/
//	     	String desc = description.get();
//	     	String flowAll = flow.get_def_all(mapProp);
//	     	String flowNoExport = flow.get_def_noExport(mapProp);
	     	String rcpt = getRCPT(tableName, odsTableName, layoutMapList, mapProp);
//	     	
//	     	// bin/
//	     	String global_variables_def = global_variables.getDef();
//	     	String b01_hql = BEFORE_C01_Run.getHQL(partition, mapProp, tableName);
//	     	String b02_hql = BEFORE_C02_Check.getHQL(partition, mapProp, tableName);
//	     	String b03_hql = BEFORE_C03_ExportDate2File.getHQL(partition, mapProp, tableName);
//	     	String b04_hql = BEFORE_C04_FinishData.getHQL(partition, mapProp, tableName);
//	     	String b01_var = BEFORE_C01_Run.getVAR(mapProp, tableName);
//	     	String b02_var = BEFORE_C02_Check.getVAR(partition, mapProp, tableName);
//	     	String b03_var = BEFORE_C03_ExportDate2File.getShellVAR(tableName);
//	     	String b04_var = BEFORE_C04_FinishData.getShellVAR(tableName);
//	     	String b03_sh = BEFORE_C03_ExportDate2File.getShell(tableName);
//	     	String b04_sh = BEFORE_C04_FinishData.getShell(tableName);
//	     	String backup_dw_hql = BACKUP_DW.getHQL(partition, mapProp, tableName, type);
//	     	String backup_dw_var = BACKUP_DW.getVAR(mapProp, tableName, type);
//	     	String dw_export_2sql_dbc = DW_EXPORT_2SQL.getDBC(type, mapProp, tableName);
//	     	String dw_export_2sql_sh = DW_EXPORT_2SQL.getShell(type, mapProp, tableName);
//	     	String dw_export_2sql_var = DW_EXPORT_2SQL.getShellVAR(type, mapProp, tableName);
//	     	String dw_l08_loaddw_tmp_hql = DW_L08_LoadDW_TMP.getHQL(partition, mapProp, tableName, selectStr, type);
//	     	String dw_l08_loaddw_tmp_var = DW_L08_LoadDW_TMP.getVAR(mapProp, tableName, type);
//	     	
//	     	if("1".equals(runType)) {
//		     	String file_upload_sh = FILE_UPLOAD.getShell(mapProp, tableName, txtFileName);
//		     	String file_upload_var = FILE_UPLOAD.getShellVAR(tableName);
//		     	String o01_hql = ODS_C01_UploadFile.getHQL(mapProp, tableName, odsTableName);
//		     	String o01_var = ODS_C01_UploadFile.getVAR(mapProp, tableName, odsTableName);
//		     	String o02_hql = ODS_C02_GetDataFileName.getHQL(mapProp, tableName, odsTableName);
//		     	String o02_var = ODS_C02_GetDataFileName.getVAR(mapProp, tableName, odsTableName);
//		     	String o03_hql = ODS_C03_GetErrorData.getHQL(mapProp, tableName, odsTableName, finalLen, hasChinese);
//		     	String o03_var = ODS_C03_GetErrorData.getVAR(mapProp, tableName, odsTableName, finalLen);
//		     	String o04_hql = ODS_C04_GetData.getHQL(mapProp, tableName, odsTableName, finalLen, hasChinese);
//		     	String o04_var = ODS_C04_GetData.getVAR(mapProp, tableName, odsTableName, finalLen);
//		     	String o05_hql = ODS_C05_GetDoneFile.getHQL(mapProp, tableName, odsTableName, finalLen, hasChinese);
//		     	String o05_var = ODS_C05_GetDoneFile.getVAR(mapProp, tableName, odsTableName, finalLen);
//		     	String truncate_ods_hql = TRUNCATE_ODS.getHQL(mapProp, tableName, odsTableName);
//		     	String truncate_ods_var = TRUNCATE_ODS.getVAR(mapProp, tableName, odsTableName);
//		     	
//
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C01_UploadFile", "hql", o01_hql);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C02_GetDataFileName", "hql", o02_hql);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C03_GetErrorData", "hql", o03_hql);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C04_GetData", "hql", o04_hql);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C05_GetDoneFile", "hql", o05_hql);
//				FileTools.createFileNotAppend(outputPathBin, "TRUNCATE_ODS", "hql", truncate_ods_hql);
//	
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C01_UploadFile", "var", o01_var);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C02_GetDataFileName", "var", o02_var);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C03_GetErrorData", "var", o03_var);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C04_GetData", "var", o04_var);
//				FileTools.createFileNotAppend(outputPathBin, "ODS_C05_GetDoneFile", "var", o05_var);
//				FileTools.createFileNotAppend(outputPathBin, "FILE_UPLOAD", "var", file_upload_var);
//				FileTools.createFileNotAppend(outputPathBin, "TRUNCATE_ODS", "var", truncate_ods_var);
//	
//				FileTools.createFileNotAppend(outputPathBin, "FILE_UPLOAD", "sh", file_upload_sh);
//	     	} else {
//	     		String dm_l02_loaddm_tmp_hql = DM_L02_LoadDM.getHQL(partition, mapProp, tableName);
//		     	String dm_l02_loaddm_tmp_var = DM_L02_LoadDM.getVAR(mapProp, tableName);
//		     	
//		     	FileTools.createFileNotAppend(outputPathBin, "DM_L02_LoadDM", "hql", dm_l02_loaddm_tmp_hql);
//				FileTools.createFileNotAppend(outputPathBin, "DM_L02_LoadDM", "var", dm_l02_loaddm_tmp_var);
//	     	}
//	     	
//	     	String finish_hql = FINISH.getHQL(mapProp, tableName);
//	     	String finish_var = FINISH.getVAR(mapProp, tableName);
//	     	String truncate_dw_hql = TRUNCATE_DW.getHQL(partition, mapProp, tableName, type);
//	     	String truncate_dw_sh = TRUNCATE_DW.getShell(tableName, type);
//	     	String truncate_dw_var = TRUNCATE_DW.getShellVAR(tableName, type);
	     	
//			FileTools.copyFile(outputPath+"../../CopyFile",outputPath); // 將main檔案copy進來
			FileTools.createFileNotAppend(outputPath, "RCPT", "hql", rcpt);
//			FileTools.createFileNotAppend(outputPath, "description", "txt", desc);
//			FileTools.createFileNotAppend(outputPath, "flow.def_all", "txt", flowAll);
//			FileTools.createFileNotAppend(outputPath, "flow.def_noExport", "txt", flowNoExport);
//			
//			// bin/ 
//			// HQL
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C01_Run", "hql", b01_hql);
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C02_Check", "hql", b02_hql);
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C03_ExportDate2File", "hql", b03_hql);
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C04_FinishData", "hql", b04_hql);
//			FileTools.createFileNotAppend(outputPathBin, "BACKUP_"+type, "hql", backup_dw_hql);
			String load_tmp_codeFileName = type+"_L0"+("DW".equals(type) ? "8" : "3")+"_Load"+type+"_TMP";
//			FileTools.createFileNotAppend(outputPathBin, load_tmp_codeFileName, "hql", dw_l08_loaddw_tmp_hql);
//			FileTools.createFileNotAppend(outputPathBin, "FINISH", "hql", finish_hql);
//			FileTools.createFileNotAppend(outputPathBin, "TRUNCATE_"+type, "hql", truncate_dw_hql);
//
//			// VAR
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C01_Run", "var", b01_var);
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C02_Check", "var", b02_var);
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C03_ExportDate2File", "var", b03_var);
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C04_FinishData", "var", b04_var);
//			FileTools.createFileNotAppend(outputPathBin, "BACKUP_"+type, "var", backup_dw_var);
//			FileTools.createFileNotAppend(outputPathBin, type+"_EXPORT_2SQL", "var", dw_export_2sql_var);
//			FileTools.createFileNotAppend(outputPathBin, load_tmp_codeFileName, "var", dw_l08_loaddw_tmp_var);
//			FileTools.createFileNotAppend(outputPathBin, "FINISH", "var", finish_var);
//			FileTools.createFileNotAppend(outputPathBin, "TRUNCATE_"+type, "var", truncate_dw_var);
//			
//			// SH
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C03_ExportDate2File", "sh", b03_sh);
//			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C04_FinishData", "sh", b04_sh);
//			FileTools.createFileNotAppend(outputPathBin, type+"_EXPORT_2SQL", "sh", dw_export_2sql_sh);
//			FileTools.createFileNotAppend(outputPathBin, "TRUNCATE_"+type, "sh", truncate_dw_sh);
//			
//			// Other
//			FileTools.createFileNotAppend(outputPathBin, type+"_EXPORT_2SQL", "dbc", dw_export_2sql_dbc);
//			FileTools.createFileNotAppend(outputPathBin, "global-variables", "def", global_variables_def);
			
			

			/******************
			 * airflow
			 ******************/
	     	String b01_hql_airflow = BEFORE_C01_Run.getHQL_airflow(partition, mapProp, tableName);
	     	String backup_dw_hql_airflow = BACKUP_DW.getHQL_airflow(partition, mapProp, tableName, type);
	     	String dw_export_2sql_dbc_airflow = DW_EXPORT_2SQL.getDBC(type, mapProp, tableName);
//	     	String dw_export_2sql_sh_airflow = DW_EXPORT_2SQL.getShell(type, mapProp, tableName);
	     	String dw_l08_loaddw_tmp_hql_airflow = DW_L08_LoadDW_TMP.getHQL_airflow(partition, mapProp, tableName, selectStr, type);
	     	
	     	if("1".equals(runType)) {
		     	String python_airflow = Python_Airflow.getDW(tableName, txtFileName, layoutMap.get("SourceFileIsZip"));
		     	String o01_hql_airflow = ODS_C01_UploadFile.getHQL_airflow(mapProp, tableName, odsTableName);
		     	String o02_hql_airflow = ODS_C02_GetDataFileName.getHQL_airflow(mapProp, tableName, odsTableName);
		     	String o03_hql_airflow = ODS_C03_GetErrorData.getHQL_airflow(mapProp, tableName, odsTableName, finalLen, hasChinese);
		     	String o04_hql_airflow = ODS_C04_GetData.getHQL_airflow(mapProp, tableName, odsTableName, finalLen, hasChinese);
		     	String o05_hql_airflow = ODS_C05_GetDoneFile.getHQL_airflow(mapProp, tableName, odsTableName, finalLen, hasChinese);
		     	String truncate_ods_hql_airflow = TRUNCATE_ODS.getHQL_airflow(mapProp, tableName, odsTableName);
		     	

				FileTools.createFileNotAppend(outputPathBin, "ODS_C01_UploadFile", "hql", o01_hql_airflow);
				FileTools.createFileNotAppend(outputPathBin, "ODS_C02_GetDataFileName", "hql", o02_hql_airflow);
				FileTools.createFileNotAppend(outputPathBin, "ODS_C03_GetErrorData", "hql", o03_hql_airflow);
				FileTools.createFileNotAppend(outputPathBin, "ODS_C04_GetData", "hql", o04_hql_airflow);
				FileTools.createFileNotAppend(outputPathBin, "ODS_C05_GetDoneFile", "hql", o05_hql_airflow);
				FileTools.createFileNotAppend(outputPathBin, "TRUNCATE_ODS", "hql", truncate_ods_hql_airflow);
				FileTools.createFileNotAppend(outputPath, tableName, "py", python_airflow);

	     	} else {
		     	String python_airflow = Python_Airflow.getDM(tableName, txtFileName);
	     		String dm_l02_loaddm_tmp_hql_airflow = DM_L02_LoadDM.getHQL_airflow(partition, mapProp, tableName);
		     	
		     	FileTools.createFileNotAppend(outputPathBin, "DM_L02_LoadDM", "hql", dm_l02_loaddm_tmp_hql_airflow);
				FileTools.createFileNotAppend(outputPath, tableName, "py", python_airflow);
	     	}
	     	
	     	String truncate_dw_hql_airflow = TRUNCATE_DW.getHQL_airflow(partition, mapProp, tableName, type);
	     	
			// bin/ 
			// HQL
			FileTools.createFileNotAppend(outputPathBin, "BEFORE_C01", "hql", b01_hql_airflow);
			FileTools.createFileNotAppend(outputPathBin, "BACKUP_"+type, "hql", backup_dw_hql_airflow);
			FileTools.createFileNotAppend(outputPathBin, load_tmp_codeFileName, "hql", dw_l08_loaddw_tmp_hql_airflow);
			FileTools.createFileNotAppend(outputPathBin, "TRUNCATE_"+type, "hql", truncate_dw_hql_airflow);
			
			// SH
//			FileTools.createFileNotAppend(outputPathBin, type+"_EXPORT_2SQL", "sh", dw_export_2sql_sh_airflow);
			
			// Other
			FileTools.createFileNotAppend(outputPathBin, "EXPORT_2SQL", "dbc", dw_export_2sql_dbc_airflow);

		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
	

	// 整理RCPT的SQL
	private static String getRCPT(String tableName, String odsTableName, List<Map<String, String>> layoutMapList,
			Map<String, String> mapProp) throws Exception {
		
		try {
			List<String> charTypeList = Arrays.asList(new String[] { "VARCHAR", "CHAR" });
			List<String> intTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER" });
	        
			// list的最後一筆位置
			String rcptODSColLogic = "", colLogic = "", rcptColLogic = "";
			for (Map<String, String> layoutMap : layoutMapList) {
				if ("Detail".equals(layoutMap.get("MapType"))) {
					String colEName = layoutMap.get("ColEName").toString().toUpperCase();
					String colType = layoutMap.get("ColType").toString().toUpperCase();
					String colLen = layoutMap.get("ColLen").toString().toUpperCase();

					if (intTypeList.contains(colType) || "DECIMAL".equals(colType)) {
						// 欄位型態轉換
						colLogic = WriteToLogic.getColLogic(charTypeList, intTypeList, colEName, colType, colLen);
						rcptODSColLogic += "\tnvl(sum(" + colLogic + "),0) as " + colEName + " ,\n";
						rcptColLogic += "\tnvl(sum(" + colEName + "),0) as " + colEName + " ,\n";
					}
				}
			}
			
			rcptODSColLogic = StringUtils.isBlank(rcptODSColLogic) ? "" : rcptODSColLogic.substring(0,rcptODSColLogic.length() - 2);
			rcptColLogic = StringUtils.isBlank(rcptColLogic) ? "" : rcptColLogic.substring(0,rcptColLogic.length() - 2);
			String sql = RCPT.getHQL(mapProp, rcptODSColLogic, rcptColLogic, odsTableName, tableName);
			
			return sql;
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}
	}
}
