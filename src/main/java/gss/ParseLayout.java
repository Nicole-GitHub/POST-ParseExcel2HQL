package gss;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class ParseLayout {
	private static final String className = ParseLayout.class.getName();
	
	/**
	 * 取得 Layout 內容
	 * @param sheetLayout
	 * @param dbname
	 * @return
	 * @throws Exception
	 */
	public static Map<String, String> run (Sheet sheetLayout, Map<String, String> mapProp) throws Exception {
		Row row = null;
		String rsMSSQL = "", rsHADOOP = "", rsCols = "", rsHPCols = "", rsMSCols = "", pkStr = "", rsCreatePartition = "";
		boolean isPartition = false;
		Map<String, String> mapReturn = new HashMap<String, String>();
		Map<String, String> mapCreatePartition = new HashMap<String, String>();
		List<Map<String,String>> rsCreatePartitionList = new ArrayList<Map<String,String>>();
		// 在Create Table Script 中不需寫長度的 DataType
		List<String> len0Typelist = Arrays.asList(new String[] {"DATE","TIMESTAMP","INTEGER","SMALLINT","BIGINT"});

		try {
			String tableName = Tools.getCellValue(sheetLayout.getRow(0), 4, "TABLE名稱");
			String partition = Tools.getCellValue(sheetLayout.getRow(0), 10, "Partition");
			String[] partitionList = partition.split(",");
			
			// 解析資料內容(從第五ROW開頭爬)
			for (int r = 4; r <= sheetLayout.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetLayout.getRow(r);
				if (row == null || !Tools.isntBlank(row.getCell(1)))
					break;
				
				String colEName = Tools.getCellValue(row, c++, "欄位英文名稱");
				c++;// 欄位中文名稱
				String type = Tools.getCellValue(row, c++, "資料型態");
				String len = Tools.getCellValue(row, c++, "資料長度");
				String pk = Tools.getCellValue(row, c++, "主鍵註記").toUpperCase();
				String nullable = Tools.getCellValue(row, c++, "NULL註記").toUpperCase();
				String init = Tools.getCellValue(row, c++, "初始值");

				type = "DATETIME".equalsIgnoreCase(type) ? "TIMESTAMP" : type;
				len = len0Typelist.contains(type.toUpperCase()) ? "" : "(" + len + ")";
				init = StringUtils.isBlank(init) || "IDENTITY(1,1)".equals(init) ? init : "DEFAULT " + init;

				rsCols = "\t" + colEName + " " + type + len;
				
				// HADOOP
				// Partiton欄位的位置要另外放
				isPartition = false;
				for(String str :partitionList) {
					if(colEName.equals(str)) {
						mapCreatePartition = new HashMap<String, String>();
						mapCreatePartition.put("Col", colEName);
						mapCreatePartition.put("Script", rsCols + " ,\n");
						rsCreatePartitionList.add(mapCreatePartition);
						isPartition = true;
					}
				}

				// 非Partiton欄位的位置正常
				if (!isPartition) {
					rsHPCols += rsCols + " " + ("N".equals(nullable) ? "NOT NULL" : "") + " " + init + " ,\n";
				}
				
				// MSSQL
				rsMSCols += rsCols + " " + ("N".equals(nullable) ? "NOT NULL" : "NULL") + " " + init + " ,\n";
				pkStr += "Y".equals(pk) ? colEName + "," : "";
			}
			
			// MSSQL CREATE TABLE Script
			rsMSSQL = "-- MSSQL \nCREATE TABLE " + mapProp.get("mssql.dbname") + ".dbo." + tableName + " (\n";
			if (StringUtils.isBlank(pkStr))
				rsMSSQL += rsMSCols.substring(0, rsMSCols.lastIndexOf(","));
			else
				rsMSSQL += rsMSCols + "\tPRIMARY KEY (" + pkStr.substring(0, pkStr.length() - 1) + ")";
			rsMSSQL += "\n);";
			
			// 確認最後輸出的partition順序需與Layout頁籤的partition欄位相同
			for (String str : partitionList) {
				for (Map<String, String> map : rsCreatePartitionList) {
					String createPartitonCol = map.get("Col").toString();
					str = str.trim();
					if (createPartitonCol.equalsIgnoreCase(str)) {
						rsCreatePartition += map.get("Script").toString().substring(1);;
						break;
					}
				}
			}

			// HADOOP CREATE TABLE Script
			rsHADOOP = "-- HADOOP \n"
					+ "DROP TABLE IF EXISTS " + mapProp.get("hadoop.raw.dbname") + "." + tableName + " ;\r\n"
					+ "CREATE TABLE IF NOT EXISTS " + mapProp.get("hadoop.raw.dbname") + "." + tableName + " (\n"
					+ rsHPCols.substring(0, rsHPCols.lastIndexOf(",")) + "\n)\n"
					+ "PARTITIONED BY(" + rsCreatePartition + " batchid BIGINT);";
			
			mapReturn.put("HPSQL", rsHADOOP);
			mapReturn.put("MSSQL", rsMSSQL);
			mapReturn.put("TableName", tableName);
			mapReturn.put("Partition", partition);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

System.out.println("ParseLayout Done!");
		return mapReturn;
	}
}
