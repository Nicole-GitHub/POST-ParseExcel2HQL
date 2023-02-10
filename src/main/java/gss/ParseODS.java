package gss;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class ParseODS {
	private static final String className = ParseODS.class.getName();
	
	/**
	 * 取得 ODS 內容
	 * @param sheetODS
	 * @param dbname
	 * @return
	 * @throws Exception
	 */
	public static Map<String, String> run (Sheet sheetODS, Map<String, String> mapProp, String partition) throws Exception {
		Row row = null;
		String rsCREATE = "", rsINSERT = "", rsCreateCols = "", rsSelectCols = "", createScript = "", selectScript = ""
				,rsCreatePartition = "", rsSelectPartition = "";
		boolean isPartition = false;
		List<Map<String, String>> rsCreatePartitionList = new ArrayList<Map<String, String>>();
		List<Map<String, String>> rsSelectPartitionList = new ArrayList<Map<String, String>>();
		Map<String, String> mapReturn = new HashMap<String, String>();
		Map<String, String> mapPartition = new HashMap<String, String>();
		
		try {
			
			// partition
			String[] partitionList = partition.split(",");
			
			// 解析資料內容(從第四ROW開頭爬)
			for (int r = 3; r <= sheetODS.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetODS.getRow(r);
				if (row == null || !Tools.isntBlank(row.getCell(1)))
					break;
				
				String dwColEName = Tools.getCellValue(row, c++, "DW欄位英文名稱");
				c++;// 來源欄位英文名稱
				int dataStart = Integer.parseInt(Tools.getCellValue(row, c++, "資料起點"));
				int dataEnd = Integer.parseInt(Tools.getCellValue(row, c++, "資料終點"));
				int datalen = dataEnd - dataStart + 1;

				createScript = "\t" + dwColEName + " VARCHAR(" + datalen + ") ,\n";
				selectScript = "TRIM(SUBSTRING(line," + dataStart + "," + datalen + "))";
				selectScript = "\tcase when " + selectScript + " = '' then NULL else " + selectScript + " end AS "
						+ dwColEName + " ,\n";
				
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
			
//			System.out.println("rsCreatePartition:"+rsCreatePartition);
//			System.out.println("rsSelectPartition:"+rsSelectPartition);

			// CREATE TABLE Script
			rsCREATE = "DROP TABLE IF EXISTS " + mapProp.get("hadoop.raw.dbname") + "." + tableName + ";\n"
					+ "CREATE TABLE IF NOT EXISTS " + mapProp.get("hadoop.raw.dbname") + "." + tableName + " (\n"
					+ rsCreateCols.substring(0, rsCreateCols.lastIndexOf(",")) + "\n)\n"
					+ "PARTITIONED BY(" + rsCreatePartition + " batchid BIGINT);";
			
			// INSERT INTO Script
			partition += StringUtils.isBlank(partition) ? "" : ",";
			rsINSERT = "INSERT OVERWRITE TABLE " + mapProp.get("hadoop.raw.dbname") + "." + tableName + " \n"
					+ "PARTITION(" + partition + " batchid) \n"
					+ "SELECT \n" + rsSelectCols + rsSelectPartition + "\tbatchid \n"
					+ "FROM " + mapProp.get("hadoop.meta.dbname") + "." + tableName + "_files \n"
					+ "WHERE batchid = ${BATCHID} ;";
			
			mapReturn.put("CreateSql", rsCREATE);
			mapReturn.put("InsertSql", rsINSERT);
			mapReturn.put("TableName", tableName);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

System.out.println("ParseODSLayout Done!");
		return mapReturn;
	}

}
