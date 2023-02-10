package gss.tableLayout;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import gss.tools.FileTools;
import gss.tools.Property;
import gss.tools.Tools;
import gss.txt.RunParseTXT;

public class RunParseTableLayout {
	private static final String className = RunParseTableLayout.class.getName();
	
	/**
	 * 取得 Excel 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception
	 */
	public static void run(String path, List<String> fileNameList) throws Exception {
		List<Map<String, String>> tableMapList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> colsMapList = new ArrayList<Map<String, String>>();
		Map<String, String> layoutMap = new HashMap<String, String>();
		Map<String, String> odsMap = new HashMap<String, String>();
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		Map<String, String> map;
		String partition = "";

		try {

			// Property
			Map<String, String> mapProp = Property.getProperties(path + "../");

			for (String fileName : fileNameList) {
				System.out.println("fileName:" + path + fileName);

				// 防呆 Excel必需要有Layout頁籤
				throwException(path + fileName);

				layoutMap = ParseLayout.run(Tools.getSheet(path + fileName, "Layout"), mapProp);
				partition = layoutMap.get("Partition");
				// 若無資料關聯或欄位處理邏輯頁籤則不產邏輯相關hql
				if (Tools.getSheet(path + fileName, "資料關聯") != null
						&& Tools.getSheet(path + fileName, "欄位處理邏輯") != null) {
					tableMapList = ParseTable.run(Tools.getSheet(path + fileName, "資料關聯"));
					colsMapList = ParseCols.run(Tools.getSheet(path + fileName, "欄位處理邏輯"), partition);
				}

				// 若無ODS頁籤則不產ODS相關hql
				if (Tools.getSheet(path + fileName, "ODS") != null) {
					odsMap = ParseODS.run(Tools.getSheet(path + fileName, "ODS"), mapProp, partition);
					RunParseTXT.run(path, odsMap.get("TXTName"), odsMap.get("DataStartEnd"));
				}
				
				fileName = fileName.substring(0, fileName.lastIndexOf("."));
				String tableType = "D" + layoutMap.get("TableName").substring(5, 6);

				/**
				 * 欄位處理邏輯 因欄位處理邏輯的table與cols分了兩個頁籤，故先至對應頁籤抓資訊後再拉回此處組合
				 */
				for (Map<String, String> mapTable : tableMapList) {
					for (Map<String, String> mapCols : colsMapList) {
						if (mapTable.get("Target").toString().equalsIgnoreCase(mapCols.get("Target").toString())) {
							String tableDBName = mapTable.get("Target").contains("{raw}.")
									? mapProp.get("hadoop.raw.dbname")
									: mapProp.get("hadoop.tmp.dbname");
							String target = mapTable.get("Target").substring(mapTable.get("Target").indexOf(".") + 1);
							String tmpTableName = tableDBName + "." + layoutMap.get("TableName")
									+ (!"TARGET".equalsIgnoreCase(target) ? "_" + target : "");
							String sql = "INSERT OVERWRITE TABLE " + tmpTableName + " \n";
							if ("TARGET".equalsIgnoreCase(target))
								sql += "PARTITION(" + (StringUtils.isBlank(partition) ? "" : partition + ",")
										+ " batchid) \n";
							sql += mapCols.get("Select") + " \n" + mapTable.get("FromWhere")
									+ (!StringUtils.isBlank(mapCols.get("Group")) ? " \n" + mapCols.get("Group") : "")
									+ (!StringUtils.isBlank(mapCols.get("Order")) ? " \n" + mapCols.get("Order") : "")
									+ ";";
							sql = sql
									.replace("{tmp}.",
											mapProp.get("hadoop.tmp.dbname") + "." + layoutMap.get("TableName") + "_")
									.replace("{raw}", mapProp.get("hadoop.raw.dbname"));
							String createSql = mapCols.get("CreateSql").toString().replace("{tmp}.",
									mapProp.get("hadoop.tmp.dbname") + "." + layoutMap.get("TableName") + "_");
							String hqlName = tableType + "_" + mapTable.get("Step");

							map = new HashMap<String, String>();
							map.put("Folder", fileName);
							map.put("HQLName", hqlName);
							map.put("SQL", sql);
							mapList.add(map);

							// Target Table 的 Create Script 用 Layout 頁籤產
							if (!StringUtils.isBlank(createSql)) {
								map = new HashMap<String, String>();
								map.put("Folder", fileName);
								map.put("HQLName", hqlName + "_create");
								map.put("SQL", createSql);
								mapList.add(map);
							}
						}
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
			}

			writeContent(path, mapList);
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
	 * 
	 * @param path
	 * @throws Exception
	 */
	private static void throwException(String path) throws Exception {
		if (Tools.getSheet(path, "Layout") == null)
			throw new Exception(className + " Error: 缺少頁韱:Layout");
	}
}
