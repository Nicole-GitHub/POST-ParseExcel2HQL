package gss;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class Parser {
	private static final String className = Parser.class.getName();

	public static void main(String args[]) throws Exception {
		
		String os = System.getProperty("os.name");

		System.out.println("=== NOW TIME: " + new Date());
		System.out.println("=== os.name: " + os);
		
		// 判斷當前執行的啟動方式是IDE還是jar
		boolean isStartupFromJar = new File(Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath()).isFile();
		System.out.println("=== isStartupFromJar: " + isStartupFromJar);
		String fileName = "";
		String path = System.getProperty("user.dir") + File.separator; // Jar
		if(!isStartupFromJar) {// IDE
			path = os.contains("Mac") ? "/Users/nicole/Dropbox/POST/ParseExcel2HQL/" // Mac
							: "C:/Users/nicole_tsou/Dropbox/POST/ParseExcel2HQL/"; // win

			fileName = "檔案定義檔.xlsx|檔案定義檔2.xlsx";
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

System.out.println("path: " + path + "\r\n fileName: " + fileName);

		String[] fileNameList = fileName.split("\\|");
//		writeContent (path, runParse(path, fileNameList));
		runParse(path, fileNameList);
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
			FileTools.createFile(path + "/" + folderName + "/", "get_" + map.get("Target"), "sql", map.get("SQL"));
		}
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
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		Map<String, String> map;
		String layout = "";
		
		try {
			for (String fileName : fileNameList) {
				System.out.println("fileName:" + path + fileName);

				tableMapList = runParseTable(Tools.getSheet(path + fileName, "資料關聯"));
				colsMapList = runParseCols(Tools.getSheet(path + fileName, "欄位處理邏輯"));
				layout = runParseLayout(Tools.getSheet(path + fileName, "Layout"));
				
				fileName = fileName.substring(0,fileName.lastIndexOf("."));
				for (Map<String, String> mapTable : tableMapList) {
					for (Map<String, String> mapCols : colsMapList) {
						if (mapTable.get("Target").toString().equals(mapCols.get("Target").toString())) {
							map = new HashMap<String, String>();
							map.put("Folder", fileName);
							map.put("Target", mapTable.get("Target"));
							map.put("SQL",
									"INSERT INTO " + mapTable.get("Target") + " \r\n" + mapCols.get("Select") + " \r\n"
											+ mapTable.get("FromWhere") + " \r\n" + mapCols.get("Group") + " \r\n"
											+ mapCols.get("Order"));
							mapList.add(map);
						}
					}
				}
				map = new HashMap<String, String>();
				map.put("Folder", fileName);
				map.put("Target", fileName);
				map.put("SQL",layout);
				mapList.add(map);
			}
			

			writeContent (path, mapList);
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

		System.out.println("runParse Done!");
//		return mapList;
	}

	/**
	 * 取得 資料關聯 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception 
	 */
	private static List<Map<String, String>> runParseTable(Sheet sheetTable) throws Exception {
		Row row = null;
		Cell cell = null;
		int rowcount = 0;
		
		List<Map<String, String>> mapListTable = new ArrayList<Map<String, String>>();
		Map<String, String> map = null;

		try {
			// 找出欲解析的資料有幾行
			for (int i = 0; i < sheetTable.getLastRowNum(); i++) {
				cell = sheetTable.getRow(i).getCell(0);
				if (cell != null && "T".equals(cell.toString())) {
					rowcount = i;
					break;
				}
			}

			// 解析資料內容(從第二ROW開頭爬)
			for (int r = 1; r <= rowcount; r++) {
				String rs = "", rsJoinOn = "";
				map = new HashMap<String, String>();
				int c = 1; // 從第二CELL開頭爬
				row = sheetTable.getRow(r);

				String target = getCellValue(row, c++, "目的");
				// ================= 資料表1 ===================
				String table1 = getCellValue(row, c++, "資料表");
				String table1Alias = getCellValue(row, c++, "別名");
				String table1JoinCols = getCellValue(row, c++, "JOIN欄位");
				String table1Where = getCellValue(row, c++, "條件");
				String joinType = getCellValue(row, c++, "關聯");
				// ================= 資料表2 ===================
				String table2 = getCellValue(row, c++, "資料表");
				String table2Alias = getCellValue(row, c++, "別名");
				String table2JoinCols = getCellValue(row, c++, "JOIN欄位");
				String table2Where = getCellValue(row, c++, "條件");

				String[] table1JoinColsArr = table1JoinCols.split(",");
				String[] table2JoinColsArr = table2JoinCols.split(",");
				int table1JoinColsNum = table1JoinColsArr.length;
				int table2JoinColsNum = table2JoinColsArr.length;

				if (table1JoinColsNum != table2JoinColsNum)
					throw new Exception("JOIN欄位數量兩邊不一致 左邊:" + table1JoinColsNum + ",右邊:" + table2JoinColsNum);

				for (int i = 0; i < table1JoinColsNum; i++) {
					rsJoinOn += (i > 0 ? " AND " : " ON ") + table1Alias + "." + table1JoinColsArr[i] + "=" + table2Alias
							+ "." + table2JoinColsArr[i] + " ";
				}

				String rsFrom = "FROM " + table1 + " " + table1Alias + " " 
						+ (StringUtils.isBlank(joinType) ? " "
								: join2Sql(joinType) + " " + table2 + " " + table2Alias + rsJoinOn);
				String rsWhere = (StringUtils.isBlank(table1Where) && StringUtils.isBlank(table2Where)) ? " "
								:" WHERE " + (StringUtils.isBlank(table1Where) ? table2Where : table1Where) 
								+ (StringUtils.isBlank(table2Where) ? " " : " AND " + table2Where);

				rs = rsFrom + "\r\n" + rsWhere;

				map.put("Target", target);
				map.put("FromWhere", rs);
				mapListTable.add(map);
			}
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println("ParseTable Done!");
		return mapListTable;
	}
	
	/**
	 * 取得 欄位處理邏輯 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception 
	 */
	private static List<Map<String, String>> runParseCols(Sheet sheetCols) throws Exception {
		Row row = null;
		List<Map<String, String>> mapListCols = new ArrayList<Map<String, String>>();

		try {
			// 解析資料內容(從第二ROW開頭爬)
			String targetColsOld = "", targetColsNew = "", rsSelect = "", rsGroup = "";
			Map<Integer,String> mapOrder = new TreeMap<Integer,String>();
			for (int r = 1; r <= sheetCols.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetCols.getRow(r);
				
				c++; //來源
				String col = getCellValue(row, c++, "欄位處理邏輯");
				String group = getCellValue(row, c++, "群組");
				String order = getCellValue(row, c++, "排序欄位");
				String colAlias = getCellValue(row, c++, "欄位");
				c += 2;
				String targetCols = getCellValue(row, c++, "目的");
				targetColsNew = StringUtils.isBlank(targetCols) ? targetColsOld : targetCols;
				
				if(StringUtils.isBlank(targetColsOld)) 
					targetColsOld = targetColsNew;
				
				if (!targetColsOld.equals(targetColsNew)) {
					
					mapListCols.add(saveMapListCols(targetColsOld,  rsSelect,  rsGroup, mapOrder));
					rsSelect = "";
					rsGroup = "";
					mapOrder = new TreeMap<Integer,String>();
					targetColsOld = targetColsNew;
				}
				
				rsSelect += col + " as " + colAlias + " ,";
				rsGroup += "Y".equals(group) ? col + " ," : "";
				// order
				if(!StringUtils.isBlank(order)) {
					order =  col + " "+order;
					if(order.contains(",")) {
						String[] orderArr = order.split(",");
						mapOrder.put(Integer.parseInt(orderArr[1]), orderArr[0]);
					}else {
						mapOrder.put(r, order);
					}
				}
			}
			
			mapListCols.add(saveMapListCols(targetColsOld,  rsSelect,  rsGroup, mapOrder));
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

System.out.println("ParseCols Done!");
		return mapListCols;
	}
	
	/**
	 * 取得 Layout 內容
	 * @param sheetLayout
	 * @return
	 * @throws Exception
	 */
	private static String runParseLayout(Sheet sheetLayout) throws Exception {
		Row row = null;
		String rsMSSQL = "", rsHADOOP = "", rsCols = "", pkStr = "";
		
		try {
			// 解析資料內容(從第五ROW開頭爬)
			for (int r = 4; r <= sheetLayout.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetLayout.getRow(r);
				if (row == null || !Tools.isntBlank(row.getCell(1)))
					break;
				
				String colEName = getCellValue(row, c++, "欄位英文名稱");
				c++;// 欄位中文名稱
				String type = getCellValue(row, c++, "資料型態");
				String len = getCellValue(row, c++, "資料長度");
				String pk = getCellValue(row, c++, "主鍵註記").toUpperCase();
				String nullable = getCellValue(row, c++, "NULL註記").toUpperCase();
				String init = getCellValue(row, c++, "初始值");
				
				len = "0".equals(len) || StringUtils.isBlank(len) ? "" : "(" + len + ")";
				nullable = "N".equals(nullable) || StringUtils.isBlank(nullable) ? "NOT NULL" : "NULL";
				init = StringUtils.isBlank(init) || "IDENTITY(1,1)".equals(init) ? init : "DEFAULT " + init;

				rsCols += "\t" + colEName + " " + type + len + " " + nullable + " " + init + " ,\r\n";
				pkStr += "Y".equals(pk) ? colEName + "," : "";
			}
			String tableName = getCellValue(sheetLayout.getRow(0), 4, "TABLE名稱");
			
			// MSSQL CREATE TABLE Script
			rsMSSQL = "-- MSSQL \r\nCREATE TABLE dbo." + tableName + " (\r\n";
			if (StringUtils.isBlank(pkStr))
				rsMSSQL += rsCols.substring(0, rsCols.lastIndexOf(","));
			else
				rsMSSQL += rsCols + "\tPRIMARY KEY (" + pkStr.substring(0, pkStr.length() - 1) + ")";
			rsMSSQL += "\r\n);\r\n\r\n";
			
			// HADOOP CREATE TABLE Script
			rsHADOOP = "-- HADOOP \r\nCREATE TABLE dbo." + tableName + " (\r\n";
			if (StringUtils.isBlank(pkStr))
				rsHADOOP += rsCols.substring(0, rsCols.lastIndexOf(","));
			else
				rsHADOOP += rsCols + "\tPRIMARY KEY (" + pkStr.substring(0, pkStr.length() - 1) + ")";
			rsHADOOP += "\r\n);";
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

System.out.println("ParseLayout Done!");
		return rsMSSQL + rsHADOOP;
	}
	
	/**
	 * 整理Cols欲存入Map的內容
	 * @param tableJoinCols
	 * @return
	 */
	private static Map<String, String> saveMapListCols(String targetColsOld,String rsSelect,String rsGroup,Map<Integer, String> mapOrder) {
		Map<String, String> map = new HashMap<String, String>();
		String rsOrder = "";
		map.put("Target", targetColsOld);
		map.put("Select", "Select " + rsSelect.substring(0,rsSelect.length() - 1));
		map.put("Group", StringUtils.isBlank(rsGroup) ? "" : "Group by " + rsGroup.substring(0,rsGroup.length() - 1));
		// order
		for(Entry<Integer,String> set : mapOrder.entrySet()) {
			rsOrder += set.getValue() + " ,";
		}
		map.put("Order", StringUtils.isBlank(rsOrder) ? "" : "Order by " + rsOrder.substring(0,rsOrder.length() - 1));

		return map;
	}
	
	/**
	 * 將Join寫法轉換成Sql寫法
	 * @param tableJoinCols
	 * @return
	 */
	private static String join2Sql(String tableJoinCols) {
		String rs = ">=".equals(tableJoinCols) ? "Left outer join"
				: "=".equals(tableJoinCols) ? "inner join"
						: "<=".equals(tableJoinCols) ? "rigth outer join"
								: "<=>".equals(tableJoinCols) ? "full outer join" : "";
		return rs;
	}

	/**
	 * 取Excel欄位值
	 * 
	 * @param sheet
	 * @param rownum
	 * @param cellnum
	 * @param fieldName
	 * @return
	 * @throws Exception 
	 */
	private static String getCellValue(Row row, int cellnum, String fieldName) throws Exception {
		try {
			if (!Tools.isntBlank(row.getCell(cellnum))) {
				return "";
			} else if (row.getCell(cellnum).getCellType() == Cell.CELL_TYPE_NUMERIC) {
				return String.valueOf((int) row.getCell(cellnum).getNumericCellValue()).trim();
			} else if (row.getCell(cellnum).getCellType() == Cell.CELL_TYPE_STRING) {
				return row.getCell(cellnum).getStringCellValue().trim();
			}
		} catch (Exception ex) {
			throw new Exception(className + " getCellValue " + fieldName + " 格式錯誤");
		}
		return "";
	}

}
