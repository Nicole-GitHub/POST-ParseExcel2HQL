package gss.TableLayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
		String rsCREATE = "", rsHQL = "", rsVAR = "", rsCreateCols = "", rsSelectCols = "", createScript = "", selectScript = ""
				,rsCreatePartition = "", rsSelectPartition = "", dataStartEnd = "", dataCols = "";
		boolean isPartition = false, hasChineseForTable = false;
		List<Map<String, String>> rsCreatePartitionList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> rsSelectPartitionList = new ArrayList<Map<String, String>>();
		Map<String, String> mapReturn = new HashMap<String, String>();
		Map<String, String> mapPartition = new HashMap<String, String>();

		// ODS一律不切partition (調整於20230801)
		partition = "";
		
		try {
			// partition
			String[] partitionList = partition.split(",");

			// 解析資料內容(從第四ROW開頭爬)
			for (int r = 3; r <= sheetODS.getLastRowNum(); r++) {
				int c = 0; // 從第二CELL開頭爬(++c)
				row = sheetODS.getRow(r);
				if (row == null || !Tools.isntBlank(row.getCell(1)))
					break;

//				boolean delLine = false;
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
				
//				// 若此行有刪除線，則整行不讀取
//				if (delLine) continue;
				
				dataCols += dwColEName + ",";
				dataStartEnd += dataStart + "," + dataEnd + ",";
				createScript = "\t" + dwColEName + " VARCHAR(" + datalen + ") ,\n";
				
				// 含有中文則需轉碼否則長度截取會出錯
				if (hasChinese) {
					selectScript = "TRIM(ENCODE(DECODE(SUBSTRING(ENCODE(SUBSTRING(line," + dataStart + "),'BIG5'),1," + datalen + "),'BIG5'),'UTF8'))";
					selectScript = "\tcase when " + selectScript + " = '' then NULL \n\t\telse " + selectScript + " end AS "
							+ dwColEName + " ,\n";
				} else {
					selectScript = "TRIM(SUBSTRING(line," + dataStart + "," + datalen + "))";
					selectScript = "\tcase when " + selectScript + " = '' then NULL else " + selectScript + " end AS "
							+ dwColEName + " ,\n";
				}
				
				// Partiton欄位的位置要另外放
				isPartition = false;
				for (String str : partitionList) {
					if (dwColEName.equals(str)) {
						mapPartition = new HashMap<String, String>();
						mapPartition.put("Col", dwColEName);
						mapPartition.put("Script", createScript);
						rsCreatePartitionList.add(mapPartition);
						mapPartition = new HashMap<String, String>();
						mapPartition.put("Col", dwColEName);
						mapPartition.put("Script", selectScript);
						rsSelectPartitionList.add(mapPartition);
						isPartition = true;
					}
				}

				// 非Partiton欄位的位置正常
				if(!isPartition) {
					rsCreateCols += createScript;
					rsSelectCols += selectScript;
				}
			}
			String tableName = Tools.getCellValue(sheetODS.getRow(0), 4, "TABLE名稱");
			String txtFileName = Tools.getCellValue(sheetODS.getRow(0), 8, "來源文字檔檔名");

			// 確認最後輸出的partition順序需與Layout頁籤的partition欄位相同
			boolean isBreak = false;
			for (String str : partitionList) {
				for (Map<String, String> rsCreate : rsCreatePartitionList) {
					for (Map<String, String> rsSelect : rsSelectPartitionList) {
						str = str.trim();
						if (rsCreate.get("Col").toString().equalsIgnoreCase(str) && rsSelect.get("Col").toString().equalsIgnoreCase(str)) {
							rsCreatePartition += rsCreate.get("Script").toString().substring(1);
							rsSelectPartition += rsSelect.get("Script").toString();
							isBreak = true;
							break;
						}
					}
					if(isBreak) break;
				}
			}
			
			// CREATE TABLE Script
			rsCREATE = CreateTable_ODS.getHQL(partitionList, mapProp, tableName, rsCreateCols, rsCreatePartition);
			
			// INSERT INTO Script
			rsHQL = ODS_L06_LoadODS.getHQL(partition, mapProp, rsSelectCols + rsSelectPartition, tableName);
			rsVAR = ODS_L06_LoadODS.getVAR(mapProp, tableName);
			
//			mapReturn.put("CreateSql", rsCREATE);
//			mapReturn.put("HQLSTR", rsHQL);
//			mapReturn.put("VARSTR", rsVAR);
			mapReturn.put("TableName", tableName);
			mapReturn.put("TXTFileName", txtFileName);
			mapReturn.put("DataStartEnd", dataStartEnd);
			mapReturn.put("DataCols", dataCols);
			mapReturn.put("HasChineseForTable", hasChineseForTable ? "Y" : "N");
			

			outputPath += fileName + "/";
			// ODS_CMMW_VSAPC_TEMP.hql
			FileTools.createFile(outputPath , tableName, "hql", rsCREATE);
			outputPath += "bin/";
//			// ODS_L06_LoadODS.hql
			FileTools.createFile(outputPath , "ODS_L06_LoadODS", "hql", rsHQL);
//			// ODS_L06_LoadODS.var
			FileTools.createFile(outputPath , "ODS_L06_LoadODS", "var", rsVAR);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapReturn;
	}

}
