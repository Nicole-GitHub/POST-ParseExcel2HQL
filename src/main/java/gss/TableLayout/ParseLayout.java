package gss.TableLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import gss.ETLCode.CreateTable_MSSQL;
import gss.ETLCode.CreateTable_T;
import gss.Tools.FileTools;
import gss.Tools.Tools;

public class ParseLayout {
	private static final String className = ParseLayout.class.getName();
	
	/**
	 * 取得 Layout 內容
	 * 最後一筆list才是組SQL所需
	 * 前面的list皆為layout資訊
	 * @param sheetLayout
	 * @param dbname
	 * @return
	 * @throws Exception
	 */
	public static List<Map<String, String>> run(String outputPath, String fileName, Sheet sheetLayout,
			Map<String, String> mapProp) throws Exception {
		Row row = null;
		String rsMSSQL = "", rsHADOOP = "", rsCols = "", rsHPCols = "", rsMSCols = "", pkStr = "", rsCreatePartition = "";
		boolean isPartition = false;
		Map<String, String> mapReturn = new HashMap<String, String>();
		Map<String, String> mapCreatePartition = new HashMap<String, String>();
		List<Map<String,String>> rsCreatePartitionList = new ArrayList<Map<String,String>>();
		List<Map<String,String>> listReturn = new ArrayList<Map<String,String>>();
		// 在Create Table Script 中不需寫長度的 DataType
		List<String> len0Typelist = Arrays.asList(new String[] {"DATE","DATETIME","INTEGER","SMALLINT","BIGINT"});

		try {
			String tableName = Tools.getCellValue(sheetLayout.getRow(0), 4, "TABLE名稱");
			String txtFileName = Tools.getCellValue(sheetLayout.getRow(1), 8, "文字檔檔名");
			String partition = Tools.getCellValue(sheetLayout.getRow(0), 8, "Partition");
			String[] partitionList = partition.split(",");
			
			// 解析資料內容(從第五ROW開頭爬)
			for (int r = 4; r <= sheetLayout.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetLayout.getRow(r);
				if (row == null || !Tools.isntBlank(row.getCell(1)))
					break;
				
				String colEName = Tools.getCellValue(row, c++, "欄位英文名稱");
				String colCName = Tools.getCellValue(row, c++, "欄位中文名稱");
				String type = Tools.getCellValue(row, c++, "資料型態");
				String len = Tools.getCellValue(row, c++, "資料長度");
				String pk = Tools.getCellValue(row, c++, "主鍵註記").toUpperCase();
				String nullable = Tools.getCellValue(row, c++, "NULL註記").toUpperCase();
				String init = Tools.getCellValue(row, c++, "初始值");

				mapReturn = new HashMap<String, String>();
				mapReturn.put("MapType", "Detail");
				mapReturn.put("ColEName", colEName);
				mapReturn.put("ColCName", colCName);
				mapReturn.put("ColLen", len);
				mapReturn.put("ColType", type);
				mapReturn.put("PK", pk);
				mapReturn.put("Nullable", nullable);
				listReturn.add(mapReturn);
				
				len = len0Typelist.contains(type.toUpperCase()) ? "" : "(" + len + ")";
				init = StringUtils.isBlank(init) || "IDENTITY(1,1)".equals(init) ? init : "DEFAULT " + init;

				rsCols = "\t" + colEName + " " + type + len;
				
				// HADOOP
				// Partiton欄位的位置要另外放
				isPartition = false;
				for(String str : partitionList) {
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
					rsHPCols += rsCols + " " + ("N".equals(nullable) ? "NOT NULL" : "") + " ,\n";
				}
				
				// MSSQL
				rsMSCols += rsCols + " " + ("N".equals(nullable) ? "NOT NULL" : "NULL") + " " + init + " ,\n";
				pkStr += "Y".equals(pk) ? colEName + "," : "";
			}
			
			// MSSQL CREATE TABLE Script
			rsMSCols = rsMSCols.replace(" CHAR(", " NCHAR(");
			rsMSSQL = CreateTable_MSSQL.getSQL(pkStr, rsMSCols, mapProp.get("mssql.dbname") + ".dbo." + tableName);
			
			// 確認最後輸出的partition順序需與Layout頁籤的partition欄位相同
			for (String str : partitionList) {
				for (Map<String, String> map : rsCreatePartitionList) {
					String createPartitonCol = map.get("Col").toString();
					str = str.trim();
					if (createPartitonCol.equalsIgnoreCase(str)) {
						rsCreatePartition += map.get("Script").toString().substring(1);
						break;
					}
				}
			}
			
			// HADOOP CREATE TABLE Script
			rsHPCols = rsHPCols.replace("DATETIME", "TIMESTAMP");
			rsHADOOP = CreateTable_T.getHQL(partitionList, rsCreatePartition, rsHPCols, mapProp.get("hadoop.raw.dbname") + "." + tableName);
			
			mapReturn = new HashMap<String, String>();
			mapReturn.put("MapType", "Main");
//			mapReturn.put("HPSQL", rsHADOOP);
//			mapReturn.put("MSSQL", rsMSSQL);
			mapReturn.put("TableName", tableName);
			mapReturn.put("Partition", partition);
			mapReturn.put("TXTFileName", txtFileName);
			listReturn.add(mapReturn);

			outputPath += fileName + "/";
//			// T_CMMW_VSAPC_TEMP.hql
			FileTools.createFile(outputPath , tableName, "hql", rsHADOOP);
//			// MS_T_CMMW_VSAPC_TEMP.sql
			FileTools.createFile(outputPath , "MS_" + tableName, "sql", rsMSSQL);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return listReturn;
	}
}
