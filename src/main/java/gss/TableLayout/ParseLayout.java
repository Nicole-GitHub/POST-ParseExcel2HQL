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
import gss.Tools.POITools;
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
			String tableName = POITools.getCellValue(sheetLayout.getRow(0), 4, "TABLE名稱");
			String txtFileName = POITools.getCellValue(sheetLayout.getRow(1), 8, "文字檔檔名");
			String partition = POITools.getCellValue(sheetLayout.getRow(0), 8, "Partition");
			String sourceFileIsZip = POITools.getCellValue(sheetLayout.getRow(1), 8, "來源文字檔是否壓縮");
			String[] partitionList = partition.split(",");

			// 解析資料內容(從第五ROW開頭爬)
			for (int r = 4; r <= sheetLayout.getLastRowNum(); r++) {
				int c = 0; // 從第二CELL開頭爬(++c)
				row = sheetLayout.getRow(r);
				if (row == null || !POITools.cellNotBlank(row.getCell(1)))
					break;

//				boolean delLine = false;
				if (POITools.isDelLine(row, ++c)) continue;
				String colEName = POITools.getCellValue(row, c, "欄位英文名稱");
				if (POITools.isDelLine(row, ++c)) continue;
				String colCName = POITools.getCellValue(row, c, "欄位中文名稱");
				if (POITools.isDelLine(row, ++c)) continue;
				String type = POITools.getCellValue(row, c, "資料型態");
				if (POITools.isDelLine(row, ++c)) continue;
				String len = POITools.getCellValue(row, c, "資料長度");
				if (POITools.isDelLine(row, ++c)) continue;
				String pk = POITools.getCellValue(row, c, "主鍵註記").toUpperCase();
				if (POITools.isDelLine(row, ++c)) continue;
				String nullable = POITools.getCellValue(row, c, "NULL註記").toUpperCase();
				if (POITools.isDelLine(row, ++c)) continue;
				String init = POITools.getCellValue(row, c, "初始值");
				for(int i = 0 ; i < 9 ; i++) ++c; // 中間跳過9欄
				if (POITools.isDelLine(row, ++c)) continue;
				String formular = POITools.getCellValue(row, c, "公式");
				
				
//				// 若此行有刪除線，則整行不讀取
//				if (delLine) continue;

				mapReturn = new HashMap<String, String>();
				mapReturn.put("MapType", "Detail");
				mapReturn.put("ColEName", colEName);
				mapReturn.put("ColCName", colCName);
				mapReturn.put("ColLen", len);
				mapReturn.put("ColType", type);
				mapReturn.put("PK", pk);
				mapReturn.put("Nullable", nullable);
				mapReturn.put("Formular", formular);
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
			rsHPCols = rsHPCols.replace("NVARCHAR", "VARCHAR");
			rsHADOOP = CreateTable_T.getHQL(partitionList, rsCreatePartition, rsHPCols, mapProp.get("hadoop.raw.dbname") + "." + tableName);
			
			mapReturn = new HashMap<String, String>();
			mapReturn.put("MapType", "Main");
			mapReturn.put("TableName", tableName);
			mapReturn.put("Partition", partition);
			mapReturn.put("SourceFileIsZip", sourceFileIsZip);
			mapReturn.put("TXTFileName", txtFileName);
			listReturn.add(mapReturn);

			outputPath += fileName + "/";
			// T_CMMW_VSAPC_TEMP.hql
			FileTools.createFileNotAppend(outputPath , tableName, "hql", rsHADOOP);
			// MS_T_CMMW_VSAPC_TEMP.sql
			FileTools.createFileNotAppend(outputPath , "MS_" + tableName, "sql", rsMSSQL);
			
			// 因測試時需先CreateTable，故整理一份所有要Create的Table在同一份文件中
			FileTools.createFileAppend(outputPath + "../" , "Hadoop_CreateTableScript"+Tools.getNOW("yyyyMMdd"), "hql", rsHADOOP);
			FileTools.createFileAppend(outputPath + "../" , "MSSQL_CreateTableScript"+Tools.getNOW("yyyyMMdd"), "sql", rsMSSQL);
			
			// INSERT SYS_CLEANCOLS
			String insCleanCols = "INSERT INTO SYS_CLEANCOLS values('"+tableName+"','"+partition+"', GETDATE() );";
			FileTools.createFileAppend(outputPath + "../" , "MSSQL_InsCleanColsScript"+Tools.getNOW("yyyyMMdd"), "sql", insCleanCols);

		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return listReturn;
	}
}
