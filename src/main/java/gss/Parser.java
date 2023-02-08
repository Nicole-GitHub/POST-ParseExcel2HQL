package gss;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

public class Parser {
	private static final String className = Parser.class.getName();

	public static void main(String args[]) throws Exception {
		
		String os = System.getProperty("os.name");

		System.out.println("=== NOW TIME: " + new Date());
		System.out.println("=== os.name: " + os);
		System.out.println("=== Parser.class.Path: " + Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath());
		// 判斷當前執行的啟動方式是IDE還是jar
//		String runPath = Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath();
//		boolean isStartupFromJar = runPath.endsWith(".jar");
		boolean isStartupFromJar = new File(Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath()).isFile();
		System.out.println("=== isStartupFromJar: " + isStartupFromJar);
		String fileName = "";
		String path = System.getProperty("user.dir") + File.separator; // Jar
//		System.out.println("=== Parser.class.Path2: "+path);
		if(!isStartupFromJar) {// IDE
			path = os.contains("Mac") ? "/Users/nicole/Dropbox/POST/POST-ParseExcel2HQL/" // Mac
							: "C:/Users/nicole_tsou/Dropbox/POST/POST-ParseExcel2HQL/"; // win

//			fileName = "檔案定義檔.xlsx|檔案定義檔2.xlsx";
			fileName = "T_IFTW_IDRIDERADD-保單基礎數據檔(意外險附約) - 複製.xlsx";
		}

		/**
		 * 透過windows的cmd執行時需將System.in格式轉為big5才不會讓中文變亂碼
		 * 即使在cmd下chcp 65001轉成utf-8也沒用
		 * 但在eclipse執行時不能轉為big5
		 */
		Scanner s = null;
		try {
			s =  isStartupFromJar ? new Scanner(System.in, "big5") : new Scanner(System.in);
			System.out.println("請輸入檔案名稱(含副檔名，若有多個請用 pipe | 隔開): ");
			fileName = "".equals(fileName) ? s.nextLine() : fileName;
		}catch(Exception ex) {
			if(s != null) s.close();
		}

System.out.println("path: " + path + "\n fileName: " + fileName);

		String[] fileNameList = fileName.split("\\|");
//		writeContent (path, runParse(path, fileNameList));
		runParse(path, fileNameList);
	}
	
	/**
	 * 取得 Excel 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception 
	 */
	private static void runParse(String path, String[] fileNameList) throws Exception {
		List<Map<String, String>> tableMapList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> colsMapList = new ArrayList<Map<String, String>>();
		Map<String, String> layoutMap = new HashMap<String, String>();
		Map<String, String> odsMap = new HashMap<String, String>();
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		Map<String, String> map;
		
		try {

			Map<String, String> mapProp = Property.getProperties(path);
			for (String fileName : fileNameList) {
				System.out.println("fileName:" + path + fileName);
				
				// 防呆 Excel必需要有Layout頁籤
				throwException(path + fileName);
				
				layoutMap = ParseLayout.run(Tools.getSheet(path + fileName, "Layout"), mapProp);

				//若無資料關聯或欄位處理邏輯頁籤則不產邏輯相關hql
				if(Tools.getSheet(path + fileName, "資料關聯") != null && Tools.getSheet(path + fileName, "欄位處理邏輯") != null)
				{
					tableMapList = ParseTable.run(Tools.getSheet(path + fileName, "資料關聯"));
					colsMapList = ParseCols.run(Tools.getSheet(path + fileName, "欄位處理邏輯"));
				}
				
				if(tableMapList.size() > 0)
					
				//若無ODS頁籤則不產ODS相關hql
				if(Tools.getSheet(path + fileName, "ODS") != null)
					odsMap = ParseODS.run(Tools.getSheet(path + fileName, "ODS"), mapProp);
				
				fileName = fileName.substring(0,fileName.lastIndexOf("."));
				// 欄位處理邏輯
				for (Map<String, String> mapTable : tableMapList) {
					for (Map<String, String> mapCols : colsMapList) {
						if (mapTable.get("Target").toString().equalsIgnoreCase(mapCols.get("Target").toString())) {
							String tableDBName = mapTable.get("Target").contains("{raw}.") ? mapProp.get("raw.dbname") : mapProp.get("tmp.dbname");
							String target = mapTable.get("Target").substring(mapTable.get("Target").indexOf(".") + 1);
							String tableName = tableDBName + "." + layoutMap.get("TableName")
									+ (!"TARGET".equalsIgnoreCase(target) ? "_" + target : "");
							String sql = "INSERT INTO " + tableName + " \n" + mapCols.get("Select") + " \n" + mapTable.get("FromWhere") 
									+ (!StringUtils.isBlank(mapCols.get("Group")) ? " \n" + mapCols.get("Group") : "")
									+ (!StringUtils.isBlank(mapCols.get("Order")) ? " \n" + mapCols.get("Order") : "");
							sql = sql.replace("{tmp}.", mapProp.get("tmp.dbname") + "." + layoutMap.get("TableName") + "_")
									.replace("{raw}", mapProp.get("raw.dbname"));

							map = new HashMap<String, String>();
							map.put("Folder", fileName);
							map.put("HQLName", "D" + layoutMap.get("TableName").substring(5,6) + "_" + mapTable.get("Step"));
							map.put("SQL",sql);
							mapList.add(map);
						}
					}
				}
				// Layout
				map = new HashMap<String, String>();
				map.put("Folder", fileName);
				map.put("HQLName", "create_HP_" + layoutMap.get("TableName"));
				map.put("SQL",layoutMap.get("HPSQL"));
				mapList.add(map);
				map = new HashMap<String, String>();
				map.put("Folder", fileName);
				map.put("HQLName", "create_MS_" + layoutMap.get("TableName"));
				map.put("SQL",layoutMap.get("MSSQL"));
				mapList.add(map);
				
				// ODS
				if (odsMap.size() > 0) {
					map = new HashMap<String, String>();
					map.put("Folder", fileName);
					map.put("HQLName", "ODS_L01_" + odsMap.get("TableName"));
					map.put("SQL", odsMap.get("INSERTSQL"));
					mapList.add(map);
					map = new HashMap<String, String>();
					map.put("Folder", fileName);
					map.put("HQLName", "create_" + odsMap.get("TableName"));
					map.put("SQL", odsMap.get("CREATESQL"));
					mapList.add(map);
				}
			}
			

			writeContent (path, mapList);
		} catch (Exception ex) {
			throw new Exception(className + " runParse Error: \n" + ex);
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

		System.out.println("runParse Done!");
//		return mapList;
	}

	/**
	 * 輸出成個別的SQL檔
	 * 
	 * @param path
	 * @param mapList
	 * @throws IOException
	 */
	private static void writeContent(String path, List<Map<String, String>> mapList) throws IOException {
		for (Map<String, String> map : mapList) {
			String folderName = map.get("Folder").toString();
//			folderName = folderName.substring(0,folderName.lastIndexOf("."));
			FileTools.createFile(path + "/" + folderName + "/", map.get("HQLName"), 
					map.get("HQLName").startsWith("create_MS_") ? "sql" : "hql", map.get("SQL"));
		}
	}

	/**
	 * 防呆
	 * @param path
	 * @throws Exception
	 */
	private static void throwException(String path) throws Exception {
//		if(Tools.getSheet(path, "資料關聯") == null)
//			throw new Exception(className + " runParse Error: 缺少頁韱:資料關聯");
//		if(Tools.getSheet(path, "欄位處理邏輯") == null)
//			throw new Exception(className + " runParse Error: 缺少頁韱:欄位處理邏輯");
		if(Tools.getSheet(path, "Layout") == null)
			throw new Exception(className + " runParse Error: 缺少頁韱:Layout");
	}
}
