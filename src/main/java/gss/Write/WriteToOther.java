package gss.Write;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import gss.ETLCode.RCPT;
import gss.ETLCode.description;
import gss.ETLCode.flow;
import gss.ETLCode.bin.BACKUP_DW;
import gss.ETLCode.bin.BEFORE_C01_Run;
import gss.ETLCode.bin.BEFORE_C02_Check;
import gss.ETLCode.bin.BEFORE_C03_ExportDate2File;
import gss.ETLCode.bin.BEFORE_C04_FinishData;
import gss.ETLCode.bin.DW_EXPORT_2SQL;
import gss.ETLCode.bin.DW_L08_LoadDW_TMP;
import gss.ETLCode.bin.FILE_UPLOAD;
import gss.ETLCode.bin.FINISH;
import gss.ETLCode.bin.ODS_C01_UploadFile;
import gss.ETLCode.bin.ODS_C02_GetDataFileName;
import gss.ETLCode.bin.ODS_C03_GetErrorData;
import gss.ETLCode.bin.ODS_C04_GetData;
import gss.ETLCode.bin.ODS_C05_GetDoneFile;
import gss.ETLCode.bin.TRUNCATE_DW;
import gss.ETLCode.bin.TRUNCATE_ODS;
import gss.ETLCode.bin.global_variables;
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
			// list的最後一筆位置
			Map<String, String> layoutMap = layoutMapList.get(layoutMapList.size() - 1);
			String tableName = layoutMap.get("TableName");
	     	String odsTableName = "ODS" + tableName.substring(1);
	     	String partition = layoutMap.get("Partition");
			String type = mapProp.get("mssql.dbname").substring(mapProp.get("mssql.dbname").lastIndexOf("_")+1).toUpperCase();
			boolean hasChinese = "Y".equals(hasChineseForTable);
			
			// 將所有欄位名稱合在一起
			String selectStr = "";
			for (Map<String, String> layoutMapfor : layoutMapList) {
				if ("Detail".equals(layoutMapfor.get("MapType"))) {
					selectStr += "\t" + layoutMapfor.get("ColEName") + ",\n";
				}
			}
	     	String desc = description.get();
	     	String flowAll = flow.get_def_all();
	     	String flowNoExport = flow.get_def_noExport();
	     	String rcpt = getRCPT(tableName, odsTableName, layoutMapList, mapProp);
	     	// bin/
	     	String global_variables_def = global_variables.getDef();
	     	String b01_hql = BEFORE_C01_Run.getHQL(partition, mapProp, tableName);
	     	String b02_hql = BEFORE_C02_Check.getHQL(partition, mapProp, tableName);
	     	String b03_hql = BEFORE_C03_ExportDate2File.getHQL(partition, mapProp, tableName);
	     	String b04_hql = BEFORE_C04_FinishData.getHQL(partition, mapProp, tableName);
	     	String b01_var = BEFORE_C01_Run.getVAR(mapProp, tableName);
	     	String b02_var = BEFORE_C02_Check.getVAR(partition, mapProp, tableName);
	     	String b03_var = BEFORE_C03_ExportDate2File.getShellVAR(tableName);
	     	String b04_var = BEFORE_C04_FinishData.getShellVAR(tableName);
	     	String b03_sh = BEFORE_C03_ExportDate2File.getShell(tableName);
	     	String b04_sh = BEFORE_C04_FinishData.getShell(tableName);
	     	String backup_dw_hql = BACKUP_DW.getHQL(partition, mapProp, tableName);
	     	String backup_dw_var = BACKUP_DW.getVAR(mapProp, tableName);
	     	String dw_export_2sql_dbc = DW_EXPORT_2SQL.getDBC(type, mapProp, tableName);
	     	String dw_export_2sql_sh = DW_EXPORT_2SQL.getShell(type, mapProp, tableName);
	     	String dw_export_2sql_var = DW_EXPORT_2SQL.getShellVAR(type, mapProp, tableName);
	     	String dw_l08_loaddw_tmp_hql = DW_L08_LoadDW_TMP.getHQL(partition, mapProp, tableName, selectStr);
	     	String dw_l08_loaddw_tmp_var = DW_L08_LoadDW_TMP.getVAR(mapProp, tableName);
	     	String file_upload_sh = FILE_UPLOAD.getShell(mapProp, tableName, txtFileName);
	     	String file_upload_var = FILE_UPLOAD.getShellVAR(tableName);
	     	String finish_hql = FINISH.getHQL(mapProp, tableName);
	     	String finish_var = FINISH.getVAR(mapProp, tableName);
	     	String o01_hql = ODS_C01_UploadFile.getHQL(mapProp, tableName, odsTableName);
	     	String o01_var = ODS_C01_UploadFile.getVAR(mapProp, tableName, odsTableName);
	     	String o02_hql = ODS_C02_GetDataFileName.getHQL(mapProp, tableName, odsTableName);
	     	String o02_var = ODS_C02_GetDataFileName.getVAR(mapProp, tableName, odsTableName);
	     	String o03_hql = ODS_C03_GetErrorData.getHQL(mapProp, tableName, odsTableName, finalLen, hasChinese);
	     	String o03_var = ODS_C03_GetErrorData.getVAR(mapProp, tableName, odsTableName, finalLen);
	     	String o04_hql = ODS_C04_GetData.getHQL(mapProp, tableName, odsTableName, finalLen, hasChinese);
	     	String o04_var = ODS_C04_GetData.getVAR(mapProp, tableName, odsTableName, finalLen);
	     	String o05_hql = ODS_C05_GetDoneFile.getHQL(mapProp, tableName, odsTableName, finalLen, hasChinese);
	     	String o05_var = ODS_C05_GetDoneFile.getVAR(mapProp, tableName, odsTableName, finalLen);
	     	String truncate_dw_hql = TRUNCATE_DW.getHQL(partition, mapProp, tableName);
	     	String truncate_dw_sh = TRUNCATE_DW.getShell(tableName);
	     	String truncate_dw_var = TRUNCATE_DW.getShellVAR(tableName);
	     	String truncate_ods_hql = TRUNCATE_ODS.getHQL(mapProp, tableName, odsTableName);
	     	String truncate_ods_var = TRUNCATE_ODS.getVAR(mapProp, tableName, odsTableName);
	     	
			outputPath += fileName + "/";
			FileTools.copyFile(outputPath+"../../CopyFile",outputPath); // 將main檔案copy進來
			FileTools.createFile(outputPath, "RCPT", "hql", rcpt);
			FileTools.createFile(outputPath, "description", "txt", desc);
			FileTools.createFile(outputPath, "flow.def_all", "txt", flowAll);
			FileTools.createFile(outputPath, "flow.def_noExport", "txt", flowNoExport);
			
			// bin/ 
			// HQL
			outputPath += "bin/";
			FileTools.createFile(outputPath, "BEFORE_C01_Run", "hql", b01_hql);
			FileTools.createFile(outputPath, "BEFORE_C02_Check", "hql", b02_hql);
			FileTools.createFile(outputPath, "BEFORE_C03_ExportDate2File", "hql", b03_hql);
			FileTools.createFile(outputPath, "BEFORE_C04_FinishData", "hql", b04_hql);
			FileTools.createFile(outputPath, "ODS_C01_UploadFile", "hql", o01_hql);
			FileTools.createFile(outputPath, "ODS_C02_GetDataFileName", "hql", o02_hql);
			FileTools.createFile(outputPath, "ODS_C03_GetErrorData", "hql", o03_hql);
			FileTools.createFile(outputPath, "ODS_C04_GetData", "hql", o04_hql);
			FileTools.createFile(outputPath, "ODS_C05_GetDoneFile", "hql", o05_hql);
			FileTools.createFile(outputPath, "BACKUP_DW", "hql", backup_dw_hql);
			FileTools.createFile(outputPath, "DW_L08_LoadDW_TMP", "hql", dw_l08_loaddw_tmp_hql);
			FileTools.createFile(outputPath, "FINISH", "hql", finish_hql);
			FileTools.createFile(outputPath, "TRUNCATE_DW", "hql", truncate_dw_hql);
			FileTools.createFile(outputPath, "TRUNCATE_ODS", "hql", truncate_ods_hql);

			// VAR
			FileTools.createFile(outputPath, "BEFORE_C01_Run", "var", b01_var);
			FileTools.createFile(outputPath, "BEFORE_C02_Check", "var", b02_var);
			FileTools.createFile(outputPath, "BEFORE_C03_ExportDate2File", "var", b03_var);
			FileTools.createFile(outputPath, "BEFORE_C04_FinishData", "var", b04_var);
			FileTools.createFile(outputPath, "ODS_C01_UploadFile", "var", o01_var);
			FileTools.createFile(outputPath, "ODS_C02_GetDataFileName", "var", o02_var);
			FileTools.createFile(outputPath, "ODS_C03_GetErrorData", "var", o03_var);
			FileTools.createFile(outputPath, "ODS_C04_GetData", "var", o04_var);
			FileTools.createFile(outputPath, "ODS_C05_GetDoneFile", "var", o05_var);
			FileTools.createFile(outputPath, "BACKUP_DW", "var", backup_dw_var);
			FileTools.createFile(outputPath, "DW_EXPORT_2SQL", "var", dw_export_2sql_var);
			FileTools.createFile(outputPath, "DW_L08_LoadDW_TMP", "var", dw_l08_loaddw_tmp_var);
			FileTools.createFile(outputPath, "FILE_UPLOAD", "var", file_upload_var);
			FileTools.createFile(outputPath, "FINISH", "var", finish_var);
			FileTools.createFile(outputPath, "TRUNCATE_DW", "var", truncate_dw_var);
			FileTools.createFile(outputPath, "TRUNCATE_ODS", "var", truncate_ods_var);
			
			// SH
			FileTools.createFile(outputPath, "BEFORE_C03_ExportDate2File", "sh", b03_sh);
			FileTools.createFile(outputPath, "BEFORE_C04_FinishData", "sh", b04_sh);
			FileTools.createFile(outputPath, "DW_EXPORT_2SQL", "sh", dw_export_2sql_sh);
			FileTools.createFile(outputPath, "FILE_UPLOAD", "sh", file_upload_sh);
			FileTools.createFile(outputPath, "TRUNCATE_DW", "sh", truncate_dw_sh);
			
			// Other
			FileTools.createFile(outputPath, "DW_EXPORT_2SQL", "dbc", dw_export_2sql_dbc);
			FileTools.createFile(outputPath, "global_variables", "def", global_variables_def);

		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
	

	public static String getRCPT(String tableName, String odsTableName, List<Map<String, String>> layoutMapList,
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
						if (intTypeList.contains(colType)) {
							rcptODSColLogic += "\tsum(" + colLogic + ") as " + colEName + " ,\n";
							rcptColLogic += "\tsum(" + colEName + ") as " + colEName + " ,\n";
						} else if ("DECIMAL".equals(colType)) {
							rcptODSColLogic += "\tsum(" + colLogic + ") as " + colEName + " ,\n";
							rcptColLogic += "\tsum(" + colEName + ") as " + colEName + " ,\n";
						}
					}
				}
			}
			
			rcptODSColLogic = StringUtils.isBlank(rcptODSColLogic) ? "" : rcptODSColLogic.substring(0,rcptODSColLogic.length() - 2);
			rcptColLogic = StringUtils.isBlank(rcptColLogic) ? "" : rcptColLogic.substring(0,rcptColLogic.length() - 2);
			String sql = RCPT.getHQL(mapProp.get("hadoop.raw.dbname"), rcptODSColLogic, rcptColLogic, odsTableName,
					tableName);
			
			return sql;
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}
	}
}
