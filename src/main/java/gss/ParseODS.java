package gss;

import java.util.HashMap;
import java.util.Map;

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
	public static Map<String, String> run (Sheet sheetODS, Map<String, String> mapProp) throws Exception {
		Row row = null;
		String rsCREATE = "", rsINSERT = "", rsCreateCols = "", rsSelectCols = "";
		Map<String, String> map = new HashMap<String, String>();
		
		try {
			// 解析資料內容(從第五ROW開頭爬)
			for (int r = 4; r <= sheetODS.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetODS.getRow(r);
				if (row == null || !Tools.isntBlank(row.getCell(1)))
					break;
				
				String colEName = Tools.getCellValue(row, c++, "欄位英文名稱");
				c++;// 欄位中文名稱
				String dataStart = Tools.getCellValue(row, c++, "資料起點");
				String datalen = Tools.getCellValue(row, c++, "資料長度");

				rsCreateCols += "\t" + colEName + " VARCHAR(" + datalen + ") NULL ,\n";
				rsSelectCols += "\tTRIM(SUBSTRING(" + dataStart + "," + datalen + ")) AS " + colEName + " ,\n";
			}
			String tableName = Tools.getCellValue(sheetODS.getRow(0), 4, "TABLE名稱");
			
			// CREATE TABLE Script
			rsCREATE = "CREATE TABLE " + mapProp.get("raw.dbname") + "." + tableName + " (\n";
			rsCREATE += rsCreateCols.substring(0, rsCreateCols.lastIndexOf(",")) + "\n);";
			
			// INSERT INTO Script
			rsINSERT = "INSERT INTO " + mapProp.get("raw.dbname") + "." + tableName + " \n"
					+ "SELECT \n" + rsSelectCols.substring(0, rsSelectCols.lastIndexOf(",")) + " \n"
					+ "FROM " + mapProp.get("meta.dbname") + "." + tableName + "_files";
			
			map.put("CREATESQL", rsCREATE);
			map.put("INSERTSQL", rsINSERT);
			map.put("TableName", tableName);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

System.out.println("ParseODSLayout Done!");
		return map;
	}

}
