package gss.TableLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import gss.SourceFile.ChkNotNullContent;
import gss.SourceFile.RunParseSourceFile;
import gss.Tools.FileTools;
import gss.Tools.Property;
import gss.Tools.Tools;
import gss.Write.WriteToExcel;

public class RunParseTableLayout {
	private static final String className = RunParseTableLayout.class.getName();
	
	/**
	 * 取得 Excel 內容
	 * 
	 * @param tableLayoutPath
	 * @param map
	 * @throws Exception
	 */
	public static void run(String tableLayoutPath, List<String> fileNameList) throws Exception {
		List<Map<String, String>> tableMapList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> colsMapList = new ArrayList<Map<String, String>>();
		Map<String, String> layoutMap = new HashMap<String, String>();
		Map<String, String> odsMap = new HashMap<String, String>();
		List<Map<String, String>> layoutMapList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		Map<String, String> map;
		String partition = "";

		try {

			// Property
			Map<String, String> mapProp = Property.getProperties(tableLayoutPath + "../");
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
				
				layoutMapList = ParseLayout.run(workbook.getSheet("Layout"), mapProp);
				layoutMap = layoutMapList.get(layoutMapList.size()-1);// 取最後一筆Main資料
				partition = layoutMap.get("Partition");
				fileName = fileName.substring(0, fileName.lastIndexOf("."));

				// 若無ODS頁籤則不產ODS相關hql
				Sheet sheetODS = workbook.getSheet("ODS");
				if (sheetODS != null) {
					odsMap = ParseODS.run(sheetODS, mapProp, partition);
					String sourceFileName = odsMap.get("SourceFileName").toString();
					if(!StringUtils.isBlank(sourceFileName)) {
						RunParseSourceFile.run(outputPath, tableLayoutPath, fileName, layoutMapList, odsMap);
						ChkNotNullContent.run(outputPath, fileName, sourceFileName, layoutMapList);
					}
				}

				// Layout
				map = new HashMap<String, String>();
				map.put("Folder", fileName);
				map.put("HQLName", "create_HP_" + layoutMap.get("TableName"));
				map.put("SQL", layoutMap.get("HPSQL"));
				mapList.add(map);
				map = new HashMap<String, String>();
				map.put("Folder", fileName);
				map.put("HQLName", "create_MS_" + layoutMap.get("TableName"));
				map.put("SQL", layoutMap.get("MSSQL"));
				mapList.add(map);

				// ODS
				if (odsMap.size() > 0) {
					map = new HashMap<String, String>();
					map.put("Folder", fileName);
					map.put("HQLName", "ODS_L01_" + odsMap.get("TableName"));
					map.put("SQL", odsMap.get("InsertSql"));
					mapList.add(map);
					map = new HashMap<String, String>();
					map.put("Folder", fileName);
					map.put("HQLName", "create_" + odsMap.get("TableName"));
					map.put("SQL", odsMap.get("CreateSql"));
					mapList.add(map);
				}

				// IFRS 17 自動產生邏輯HQL與頁籤與rcpt script
				WriteToExcel.run(outputPath, fileName, workbook, layoutMapList, partition, mapProp);
				
				// 若 非儲壽類型、無資料關聯、無欄位處理邏輯頁籤 則不讀取邏輯相關頁籤
				Sheet sheetTableLogic = workbook.getSheet("資料關聯");
				Sheet sheetColLogic = workbook.getSheet("欄位處理邏輯");
				if (sheetTableLogic != null && sheetColLogic != null) {
					tableMapList = ParseTable.run(sheetTableLogic);
					colsMapList = ParseCols.run(sheetColLogic, partition);

					// 因欄位處理邏輯的table與cols分了兩個頁籤，故先至對應頁籤抓資訊後再拉回此處組合
					mapList.addAll(BuildLogic.run(tableMapList, colsMapList, mapProp, layoutMap, partition, fileName));
				}

			}

			writeContent(outputPath, mapList);
			
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

	/**
	 * 輸出成個別的SQL檔
	 * 
	 * @param outputPath
	 * @param mapList
	 * @throws IOException
	 */
	private static void writeContent(String outputPath, List<Map<String, String>> mapList) throws IOException {
		System.out.println("\n\n============ Create HQL =================");
		for (Map<String, String> map : mapList) {
			String folderName = map.get("Folder").toString();
			FileTools.createFile(outputPath + folderName + "/", map.get("HQLName"),
					map.get("HQLName").startsWith("create_MS_") ? "sql" : "hql", map.get("SQL"));
		}
	}
}
