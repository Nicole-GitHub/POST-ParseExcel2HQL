package gss.TableLayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import gss.Tools.Tools;
import gss.Write.ChkSourceFileContent;
import gss.Write.RunParseTXTFile;
import gss.Write.WriteToDataExport;
import gss.Write.WriteToLogic;
import gss.Write.WriteToOther;

public class RunParseTableLayout {
	private static final String className = RunParseTableLayout.class.getName();
	
	/**
	 * 取得 Excel 內容
	 * 
	 * @param tableLayoutPath
	 * @param map
	 * @throws Exception
	 */
	public static void run(Map<String, String> mapProp, String tableLayoutPath, List<String> fileNameList) throws Exception {
		Map<String, String> layoutMap = new HashMap<String, String>();
		Map<String, String> odsMap = new HashMap<String, String>();
		List<Map<String, String>> layoutMapList = new ArrayList<Map<String, String>>();
		String partition = "";

		try {

			String outputPath = tableLayoutPath + "../Output/";
			
			for (String fileName : fileNameList) {
				String fileNamePath = tableLayoutPath + fileName;
				System.out.println("\n\n=============================");
				System.out.println("fileName:" + fileNamePath);
				Workbook workbook = Tools.getWorkbook(fileNamePath);
				
				// 防呆 Excel必需要有Layout頁籤
				if (workbook.getSheet("Layout") == null)
					throw new Exception(className + " Error: 缺少頁韱:Layout");
				if ("1".equals(mapProp.get("runType"))) {
					if (workbook.getSheet("資料關聯") != null || workbook.getSheet("欄位處理邏輯") != null)
						throw new Exception(className + " Error: runType為1時不可有\"資料關聯\"與\"欄位處理邏輯\"頁籤");
				}
				
				// 取得 Layout 內容，最後一筆list才是組SQL所需，前面的list皆為layout資訊
				fileName = fileName.substring(0, fileName.lastIndexOf("."));
				layoutMapList = ParseLayout.run(outputPath, fileName, workbook.getSheet("Layout"), mapProp);
				layoutMap = layoutMapList.get(layoutMapList.size()-1);// 取最後一筆Main資料
				partition = layoutMap.get("Partition");

				// 產生邏輯HQL與"資料關聯"、"欄位處理邏輯"頁籤
				WriteToLogic.run(outputPath, fileName, workbook, layoutMapList, mapProp);
				// 若為梳理才需產出ExportFile檔
				if ("Y".equalsIgnoreCase(mapProp.get("exportfile")))
					WriteToDataExport.run(outputPath, fileName, layoutMapList, mapProp);
				
				
				// runType=1,不產ODS相關資訊
				String finalLen = "0", txtFileName = "", hasChineseForTable = "";
				if ("1".equals(mapProp.get("runType"))) {
					Sheet sheetODS = workbook.getSheet("ODS");
					if (sheetODS != null) {
						odsMap = ParseODS.run(outputPath, fileName, sheetODS, mapProp, partition);
						String[] dataStartEnd = odsMap.get("DataStartEnd").split(",");
						finalLen = dataStartEnd[dataStartEnd.length -1];
						hasChineseForTable = odsMap.get("HasChineseForTable");
						
						// 將來源文字檔轉成Excel格式
						txtFileName = odsMap.get("TXTFileName").toString();
						if(!StringUtils.isBlank(txtFileName)) {
							try {
								RunParseTXTFile.parseSourceFile(tableLayoutPath, fileName, layoutMapList, odsMap);
								if("Y".equalsIgnoreCase(mapProp.get("runType")))
										ChkSourceFileContent.run(outputPath, fileName, txtFileName, layoutMapList);
							} catch(Exception ex) {
								System.out.println(className + " Warn: \n" + ex.getMessage());
							}
						}
					}
					else
						throw new Exception(className + " Error: 缺少頁韱:ODS");
				}

				WriteToOther.run(outputPath, fileName, layoutMapList, finalLen, hasChineseForTable, txtFileName, mapProp);

//				List<Map<String, String>> tableMapList = new ArrayList<Map<String, String>>();
//				List<Map<String, String>> colsMapList = new ArrayList<Map<String, String>>();
//				List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
//				if ("2".equals(mapProp.get("runType"))) {
//					// 若 非儲壽類型、無資料關聯、無欄位處理邏輯頁籤 則不讀取邏輯相關頁籤
//					Sheet sheetTableLogic = workbook.getSheet("資料關聯");
//					Sheet sheetColLogic = workbook.getSheet("欄位處理邏輯");
//					if (sheetTableLogic != null && sheetColLogic != null) {
//						tableMapList = ParseTable.run(sheetTableLogic);
//						colsMapList = ParseCols.run(sheetColLogic, partition);
//	
//						// 因欄位處理邏輯的table與cols分了兩個頁籤，故先至對應頁籤抓資訊後再拉回此處組合
//						mapList.addAll(BuildLogic.run(tableMapList, colsMapList, mapProp, layoutMap, fileName));
//					}
//				}
			}

//			writeContent(outputPath, mapList);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}
//		System.out.println("====================== tableMapList =========================");
//		for (Map<String, String> mapList : tableMapList) {
//			System.out.println("====================== mapList =========================");
//			for (Entry<String, String> set2 : mapList.entrySet()) {
//				System.out.println(set2);
//			}
//		}
//
//		System.out.println("====================== colsMapList =========================");
//		for (Map<String, String> mapList : colsMapList) {
//			System.out.println("====================== mapList =========================");
//			for (Entry<String, String> set2 : mapList.entrySet()) {
//				System.out.println(set2);
//			}
//		}

		System.out.println(className + " Done!");
	}

//	/**
//	 * 輸出成個別的SQL檔
//	 * 
//	 * @param outputPath
//	 * @param mapList
//	 * @throws Exception 
//	 */
//	private static void writeContent(String outputPath, List<Map<String, String>> mapList) throws Exception {
////		System.out.println("\n\n============ Create HQL =================");
//		String aux = ""; 
//		
//		for (Map<String, String> map : mapList) {
//			if(map.get("HQLName").startsWith("MS_"))
//				aux = "sql";
//			else if(map.get("HQLName").endsWith(".var"))
//				aux = "var";
//			else
//				aux = "hql";
//			
//			String fileName = map.get("HQLName").endsWith(".var") ? map.get("HQLName").substring(0,map.get("HQLName").lastIndexOf(".")) : map.get("HQLName");
//			String folderName = map.get("Folder").toString();
//			FileTools.createFile(outputPath + folderName + "/", fileName, aux, map.get("SQL"));
//		}
//	}
}
