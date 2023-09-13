package gss.TableLayout;

import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import gss.ETLCode.CreateTable_ODS;
import gss.ETLCode.bin.ODS_L06_LoadODS;
import gss.Tools.FileTools;
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
				if (row == null || !Tools.isntBlank(row.getCell(1)))
					break;

				if (Tools.isDelLine(row, ++c)) continue;
				String dwColEName =Tools.getCellValue(row, c, "DW欄位英文名稱");
				c++;// 來源欄位英文名稱
				if (Tools.isDelLine(row, ++c)) continue;
				int dataStart =Integer.parseInt(Tools.getCellValue(row, c, "資料起點"));
				if (Tools.isDelLine(row, ++c)) continue;
				int dataEnd =Integer.parseInt(Tools.getCellValue(row, c, "資料終點"));
				c++;// 備註
				int datalen = dataEnd - dataStart + 1;
				if (Tools.isDelLine(row, ++c)) continue;
				boolean hasChinese ="Y".equalsIgnoreCase(Tools.getCellValue(row, c, "是否含有中文"));
				// 若有其中一個欄位含有中文，則整份Table都算含有中文(傳給外面用的)
				hasChineseForTable = hasChineseForTable ? true : hasChinese;
				
				dataCols += dwColEName + ",";
				dataStartEnd += dataStart + "," + dataEnd + ",";
				createScript = "\t" + dwColEName + " VARCHAR(" + datalen + ") ,\n";
				

				// 含有中文則需轉碼否則長度截取會出錯
//				if (hasChineseForTable) {
//					selectChineseScript = "toChinessHead(line," + dataStart + "," + datalen
//							+ ")toChinessTail";
//					selectChineseScript = "\tcase when " + selectScript + " = '' then NULL else " + selectScript
//							+ " end AS " + dwColEName + " ,\n";
//				} else {
					selectScript = "TRIM(SUBSTRING(line," + dataStart + "," + datalen + "))";
					selectScript = "\tcase when " + selectScript + " = '' then NULL else " + selectScript
							+ " end AS " + dwColEName + " ,\n";

					selectChineseScript = "TRIM(CAST(ENCODE(DECODE(SUBSTRING(line," + dataStart + "," + datalen
							+ "),'BIG5'),'UTF8') AS STRING))";
					selectChineseScript = "\tcase when " + selectChineseScript + " = '' then NULL else "
							+ selectChineseScript + " end AS " + dwColEName + " ,\n";
//				}
				
				rsCreateCols += createScript;
				rsSelectCols += selectScript;
				rsSelectChineseCols += selectChineseScript;
			}
//			String a="TRIM(CAST(ENCODE(DECODE(SUBSTRING(line," + dataStart + "," + datalen
//					+ "),'BIG5'),'UTF8') AS STRING))";
			/**
			 *  若來源有中文時會先將文字檔轉為BIG5存入TEMP檔，
			 *  但BIG5碼下TRIM會失敗，所以必需所有欄位皆用ENCODE,DECODE方式寫
			 */
			rsSelectCols = hasChineseForTable ? rsSelectChineseCols : rsSelectCols;
			if (hasChineseForTable) {
				rsSelectCols = rsSelectCols.replace("toChinessHead", "TRIM(CAST(ENCODE(DECODE(SUBSTRING"); 
				rsSelectCols = rsSelectCols.replace("toChinessTail", ",'BIG5'),'UTF8') AS STRING))"); 
			}
						
			String tableName = Tools.getCellValue(sheetODS.getRow(0), 4, "TABLE名稱");
			String txtFileName = Tools.getCellValue(sheetODS.getRow(0), 8, "來源文字檔檔名");

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
			FileTools.createFile(outputPath , tableName, "hql", rsCREATE);
			outputPath += "bin/";
			// ODS_L06_LoadODS.hql
			FileTools.createFile(outputPath , "ODS_L06_LoadODS", "hql", rsHQL);
			// ODS_L06_LoadODS.var
			FileTools.createFile(outputPath , "ODS_L06_LoadODS", "var", rsVAR);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapReturn;
	}

}
