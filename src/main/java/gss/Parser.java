package gss;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class Parser {
	private static final String className = Parser.class.getName();

	public static void main(String args[]) throws Exception {
		
		String os = System.getProperty("os.name");

		System.out.println("=== NOW TIME: " + new Date());
		System.out.println("=== os.name: " + os);
		System.out.println("=== Parser.class.Path: " + Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath());
		// 判斷當前執行的啟動方式是IDE還是jar
		// 若放檔的路徑中有中文時執行bat會讓中文變亂碼導致使用.isFile()會失效，故用.endsWith判斷
		String runPath = Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		boolean isStartupFromJar = runPath.endsWith(".jar");
//		boolean isStartupFromJar = new File(Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath()).isFile();
		System.out.println("=== isStartupFromJar: " + isStartupFromJar);
//		String fileName = "";
		String path = System.getProperty("user.dir") + File.separator; // Jar
//		System.out.println("=== Parser.class.Path2: "+path);
		if(!isStartupFromJar) {// IDE
			path = os.contains("Mac") ? "/Users/nicole/Dropbox/POST/POST-ParseExcel2HQL/" // Mac
							: "C:/Users/nicole_tsou/Dropbox/POST/POST-ParseExcel2HQL/"; // win
		}

		path += "TableLayout/";
		// 列出TableLayout下的所有檔案(不含隱藏檔)
		List<String> fileNameList = new ArrayList<String>();
		String[] fileName = new File(path).list();
		for (String str : fileName) {
			if (new File(path + str).isHidden()) {
				System.out.println("isHidden:" + str);
			}else {
				fileNameList.add(str);
				System.out.println("isNotHidden:" + str);
			}
		}
		
System.out.println("path: " + path + "\n fileName: " + fileNameList);

		runParse(path, fileNameList);
	}
	
	/**
	 * 取得 Excel 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception 
	 */
	private static void runParse(String path, List<String> fileNameList) throws Exception {
		List<Map<String, String>> tableMapList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> colsMapList = new ArrayList<Map<String, String>>();
		Map<String, String> layoutMap = new HashMap<String, String>();
		Map<String, String> odsMap = new HashMap<String, String>();
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		Map<String, String> map;
		String partition = "";
		
		try {
			
			// Property
			Map<String, String> mapProp = Property.getProperties(path+"../");
			
			for (String fileName : fileNameList) {
				System.out.println("fileName:" + path + fileName);
				
				// 防呆 Excel必需要有Layout頁籤
				throwException(path + fileName);
				
				layoutMap = ParseLayout.run(Tools.getSheet(path + fileName, "Layout"), mapProp);
				partition = layoutMap.get("Partition");
				//若無資料關聯或欄位處理邏輯頁籤則不產邏輯相關hql
				if(Tools.getSheet(path + fileName, "資料關聯") != null && Tools.getSheet(path + fileName, "欄位處理邏輯") != null)
				{
					tableMapList = ParseTable.run(Tools.getSheet(path + fileName, "資料關聯"));
					colsMapList = ParseCols.run(Tools.getSheet(path + fileName, "欄位處理邏輯"), partition);
				}
					
				//若無ODS頁籤則不產ODS相關hql
				if(Tools.getSheet(path + fileName, "ODS") != null)
					odsMap = ParseODS.run(Tools.getSheet(path + fileName, "ODS"), mapProp, partition);
				
				fileName = fileName.substring(0,fileName.lastIndexOf("."));
				String tableType = "D" + layoutMap.get("TableName").substring(5, 6);
				
				/**
				 * 欄位處理邏輯 因欄位處理邏輯的table與cols分了兩個頁籤，故先至對應頁籤抓資訊後再拉回此處組合
				 */
				for (Map<String, String> mapTable : tableMapList) {
					for (Map<String, String> mapCols : colsMapList) {
						if (mapTable.get("Target").toString().equalsIgnoreCase(mapCols.get("Target").toString())) {
							String tableDBName = mapTable.get("Target").contains("{raw}.") ? mapProp.get("hadoop.raw.dbname") : mapProp.get("hadoop.tmp.dbname");
							String target = mapTable.get("Target").substring(mapTable.get("Target").indexOf(".") + 1);
							String tmpTableName = tableDBName + "." + layoutMap.get("TableName")
									+ (!"TARGET".equalsIgnoreCase(target) ? "_" + target : "");
							String sql = "INSERT OVERWRITE TABLE " + tmpTableName + " \n";
							if("TARGET".equalsIgnoreCase(target))
								sql += "PARTITION("	+ (StringUtils.isBlank(partition) ? "" : partition + ",") + " batchid) \n";
							sql += mapCols.get("Select") + " \n" + mapTable.get("FromWhere") 
									+ (!StringUtils.isBlank(mapCols.get("Group")) ? " \n" + mapCols.get("Group") : "")
									+ (!StringUtils.isBlank(mapCols.get("Order")) ? " \n" + mapCols.get("Order") : "")
									+ ";";
							sql = sql.replace("{tmp}.", mapProp.get("hadoop.tmp.dbname") + "." + layoutMap.get("TableName") + "_")
									.replace("{raw}", mapProp.get("hadoop.raw.dbname"));
							String createSql = mapCols.get("CreateSql").toString().replace("{tmp}.", mapProp.get("hadoop.tmp.dbname") + "." + layoutMap.get("TableName") + "_");
							String hqlName = tableType + "_" + mapTable.get("Step");
							
							map = new HashMap<String, String>();
							map.put("Folder", fileName);
							map.put("HQLName", hqlName);
							map.put("SQL",sql);
							mapList.add(map);

							// Target Table 的 Create Script 用 Layout 頁籤產
							if(!StringUtils.isBlank(createSql)) {
								map = new HashMap<String, String>();
								map.put("Folder", fileName);
								map.put("HQLName", hqlName + "_create");
								map.put("SQL",createSql);
								mapList.add(map);
							}
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
					map.put("SQL", odsMap.get("InsertSql"));
					mapList.add(map);
					map = new HashMap<String, String>();
					map.put("Folder", fileName);
					map.put("HQLName", "create_" + odsMap.get("TableName"));
					map.put("SQL", odsMap.get("CreateSql"));
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
	}

	/**
	 * 輸出成個別的SQL檔
	 * 
	 * @param path
	 * @param mapList
	 * @throws IOException
	 */
	private static void writeContent(String path, List<Map<String, String>> mapList) throws IOException {
		path += "../Output/";
		FileTools.deleteFolder(new File(path));
		for (Map<String, String> map : mapList) {
			String folderName = map.get("Folder").toString();
			FileTools.createFile(path + folderName + "/", map.get("HQLName"),
					map.get("HQLName").startsWith("create_MS_") ? "sql" : "hql", map.get("SQL"));
		}
	}

	/**
	 * 防呆
	 * @param path
	 * @throws Exception
	 */
	private static void throwException(String path) throws Exception {
		if(Tools.getSheet(path, "Layout") == null)
			throw new Exception(className + " runParse Error: 缺少頁韱:Layout");
	}
}
