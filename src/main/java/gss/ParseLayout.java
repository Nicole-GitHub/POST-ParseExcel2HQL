package gss;

import java.util.HashMap;
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
		String rsMSSQL = "", rsHADOOP = "", rsCols = "", pkStr = "";
		Map<String, String> map = new HashMap<String, String>();
		
		try {
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
				
				len = "0".equals(len) || StringUtils.isBlank(len) ? "" : "(" + len + ")";
				nullable = "N".equals(nullable) ? "NOT NULL" : "NULL";
				init = StringUtils.isBlank(init) || "IDENTITY(1,1)".equals(init) ? init : "DEFAULT " + init;

				rsCols += "\t" + colEName + " " + type + len + " " + nullable + " " + init + " ,\n";
				pkStr += "Y".equals(pk) ? colEName + "," : "";
			}
			String tableName = Tools.getCellValue(sheetLayout.getRow(0), 4, "TABLE名稱");
			
			// MSSQL CREATE TABLE Script
			rsMSSQL = "-- MSSQL \nCREATE TABLE " + mapProp.get("raw.dbname") + "." + tableName + " (\n";
			if (StringUtils.isBlank(pkStr))
				rsMSSQL += rsCols.substring(0, rsCols.lastIndexOf(","));
			else
				rsMSSQL += rsCols + "\tPRIMARY KEY (" + pkStr.substring(0, pkStr.length() - 1) + ")";
			rsMSSQL += "\n);";
			
			// HADOOP CREATE TABLE Script
			rsHADOOP = "-- HADOOP \nCREATE TABLE " + mapProp.get("raw.dbname") + "." + tableName + " (\n";
			if (StringUtils.isBlank(pkStr))
				rsHADOOP += rsCols.substring(0, rsCols.lastIndexOf(","));
			else
				rsHADOOP += rsCols + "\tPRIMARY KEY (" + pkStr.substring(0, pkStr.length() - 1) + ")";
			rsHADOOP += "\n);";
			
			map.put("HPSQL", rsHADOOP);
			map.put("MSSQL", rsMSSQL);
			map.put("TableName", tableName);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

System.out.println("ParseLayout Done!");
		return map;
	}
}
