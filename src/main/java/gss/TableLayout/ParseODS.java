package gss.TableLayout;

import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import gss.ETLCode.CreateTable_ODS;
import gss.ETLCode.bin.ODS_L06_LoadODS;
import gss.Tools.FileTools;
import gss.Tools.POITools;
import gss.Tools.Tools;

public class ParseODS {
	private static final String className = ParseODS.class.getName();
	
	/**
	 * 取得 ODS 內容
	 * @param sheetODS
	 * @param dbname
	 * @return
	 * @throws Exception
	 */
	public static Map<String, String> run(String outputPath, String fileName, Sheet sheetODS,
			Map<String, String> mapProp, String partition) throws Exception {
		Row row = null;
		String rsCREATE = "", rsHQL = "", rsVAR = "", rsCreateCols = "", rsSelectCols = "", rsSelectChineseCols = "",
				createScript = "", selectScript = "", selectChineseScript = "", rsCreatePartition = "",
				rsSelectPartition = "", dataStartEnd = "", dataCols = "";
		boolean hasChineseForTable = false;
		Map<String, String> mapReturn = new HashMap<String, String>();

		try {

			// 解析資料內容(從第四ROW開頭爬)
			for (int r = 3; r <= sheetODS.getLastRowNum(); r++) {
				int c = 0; // 從第二CELL開頭爬(++c)
				row = sheetODS.getRow(r);
				if (row == null || !POITools.cellNotBlank(row.getCell(1)))
					break;

				if (POITools.isDelLine(row, ++c)) continue;
				String dwColEName = POITools.getCellValue(row, c, "DW欄位英文名稱");
				c++;// 來源欄位英文名稱
				if (POITools.isDelLine(row, ++c)) continue;
				int dataStart = Integer.parseInt(POITools.getCellValue(row, c, "資料起點"));
				if (POITools.isDelLine(row, ++c)) continue;
				int dataEnd = Integer.parseInt(POITools.getCellValue(row, c, "資料終點"));
				c++;// 備註
				int datalen = dataEnd - dataStart + 1;
				if (POITools.isDelLine(row, ++c)) continue;
				boolean hasChinese ="Y".equalsIgnoreCase(POITools.getCellValue(row, c, "是否含有中文"));
				// 若有其中一個欄位含有中文，則整份Table都算含有中文(傳給外面用的)
				hasChineseForTable = hasChineseForTable ? true : hasChinese;
				
				dataCols += dwColEName + ",";
				dataStartEnd += dataStart + "," + dataEnd + ",";
				createScript = "\t" + dwColEName + " VARCHAR(" + datalen + ") ,\n";
				

				selectScript = "TRIM(SUBSTRING(line," + dataStart + "," + datalen + "))";
				// 若TRIM完為空則設NULL
//				selectScript = "\tcase when " + selectScript + " = '' then NULL else " + selectScript
//						+ " end AS " + dwColEName + " ,\n";
				selectScript = "\t" + selectScript + " AS " + dwColEName + " ,\n";

				// 含有中文則需轉碼否則長度截取會出錯
				selectChineseScript = "TRIM(CAST(ENCODE(DECODE(SUBSTRING(line," + dataStart + "," + datalen
						+ "),'BIG5'),'UTF8') AS STRING))";
				// 若TRIM完為空則設NULL
//				selectChineseScript = "\tcase when " + selectChineseScript + " = '' then NULL else "
//						+ selectChineseScript + " end AS " + dwColEName + " ,\n";
				selectChineseScript = "\t" + selectChineseScript + " AS " + dwColEName + " ,\n";
				
				rsCreateCols += createScript;
				rsSelectCols += selectScript;
				rsSelectChineseCols += selectChineseScript;
			}
			
			/**
			 *  若來源有中文時會先將文字檔轉為BIG5存入TEMP檔，
			 *  但BIG5碼下TRIM會失敗，所以必需所有欄位皆用ENCODE,DECODE方式寫
			 */
			rsSelectCols = hasChineseForTable ? rsSelectChineseCols : rsSelectCols;
						
			String tableName = POITools.getCellValue(sheetODS.getRow(0), 4, "TABLE名稱");
			String txtFileName = POITools.getCellValue(sheetODS.getRow(0), 8, "來源文字檔檔名");

			// CREATE TABLE Script
			rsCREATE = CreateTable_ODS.getHQL(mapProp, tableName, rsCreateCols, rsCreatePartition);
			
			// INSERT INTO Script
			rsHQL = ODS_L06_LoadODS.getHQL(mapProp, rsSelectCols + rsSelectPartition, tableName, hasChineseForTable);
			rsVAR = ODS_L06_LoadODS.getVAR(mapProp, tableName, hasChineseForTable);
			
			mapReturn.put("TableName", tableName);
			mapReturn.put("TXTFileName", txtFileName);
			mapReturn.put("DataStartEnd", dataStartEnd);
			mapReturn.put("DataCols", dataCols);
			mapReturn.put("HasChineseForTable", hasChineseForTable ? "Y" : "N");
			

			outputPath += fileName + "/";
			// ODS_CMMW_VSAPC_TEMP.hql
			FileTools.createFileNotAppend(outputPath , tableName, "hql", rsCREATE);
			// 因測試時需先CreateTable，故整理一份所有要Create的Table在同一份文件中
			FileTools.createFileAppend(outputPath + "../", "CreateTableScript"+Tools.getNOW("yyyyMMdd"), "hql", rsCREATE);
			
			outputPath += "bin/";
			// ODS_L06_LoadODS.hql
			FileTools.createFileNotAppend(outputPath , "ODS_L06_LoadODS", "hql", rsHQL);
			// ODS_L06_LoadODS.var
			FileTools.createFileNotAppend(outputPath , "ODS_L06_LoadODS", "var", rsVAR);
			
			/******************
			 * airflow
			 ******************/
			String rsHQL_airflow = ODS_L06_LoadODS.getHQL_airflow(mapProp, rsSelectCols + rsSelectPartition, tableName, hasChineseForTable);
			FileTools.createFileNotAppend(outputPath+"../airflow/" , "ODS_L06_LoadODS", "hql", rsHQL_airflow);

		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapReturn;
	}

}
