package gss;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class ParseTable {
	private static final String className = ParseTable.class.getName();
	
	/**
	 * 取得 資料關聯 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception 
	 */
	public static List<Map<String, String>> run (Sheet sheetTable) throws Exception {
		Row row = null;
		Cell cell = null;
		int rowcount = 0;
		
		List<Map<String, String>> mapListTable = new ArrayList<Map<String, String>>();
		Map<String, String> map = null;

		try {
			// 找出欲解析的資料有幾行
			for (int i = 0; i <= sheetTable.getLastRowNum(); i++) {
				row = sheetTable.getRow(i);
				if (row == null) {
					rowcount = i;
					break;
				}
				cell = row.getCell(1);
				if (cell != null && "{raw}.TARGET".equalsIgnoreCase(Tools.getCellValue(row, 1, "目的"))) {
					rowcount = i;
					break;
				}
			}

			// 解析資料內容(從第二ROW開頭爬)
			for (int r = 1; r <= rowcount; r++) {
				String rs = "", rsJoinOn = "";
				map = new HashMap<String, String>();
				int c = 0; // 從第二CELL開頭爬
				row = sheetTable.getRow(r);

				String step = Tools.getCellValue(row, c++, "步驟").toUpperCase();
				String target = Tools.getCellValue(row, c++, "目的");
				// ================= 資料表1 ===================
				String table1 = Tools.getCellValue(row, c++, "資料表");
				String table1Alias = Tools.getCellValue(row, c++, "別名");
				String table1JoinCols = Tools.getCellValue(row, c++, "JOIN欄位");
				String table1Where = Tools.getCellValue(row, c++, "條件");
				String joinType = Tools.getCellValue(row, c++, "關聯");
				// ================= 資料表2 ===================
				String table2 = Tools.getCellValue(row, c++, "資料表");
				String table2Alias = Tools.getCellValue(row, c++, "別名");
				String table2JoinCols = Tools.getCellValue(row, c++, "JOIN欄位");
				String table2Where = Tools.getCellValue(row, c++, "條件");

				String[] table1JoinColsArr = table1JoinCols.split(",");
				String[] table2JoinColsArr = table2JoinCols.split(",");
				int table1JoinColsNum = table1JoinColsArr.length;
				int table2JoinColsNum = table2JoinColsArr.length;

				if (table1JoinColsNum != table2JoinColsNum)
					throw new Exception("JOIN欄位數量兩邊不一致 左邊:" + table1JoinColsNum + ",右邊:" + table2JoinColsNum);

				for (int i = 0; i < table1JoinColsNum; i++) {
					rsJoinOn += (i > 0 ? " AND " : " \n\tON ") 
							+ table1Alias + "." + table1JoinColsArr[i] 
							+ " = " + table2Alias + "." + table2JoinColsArr[i] + " ";
				}
				
				// 若為left join或right join則將次要table的where內容放在on裡
				if (!StringUtils.isBlank(rsJoinOn)) {
					if ("<=".equals(joinType)) {
						rsJoinOn += !StringUtils.isBlank(table1Where) ? " AND " + table1Where : "";
						table1Where = "";
					} else if (">=".equals(joinType)) {
						rsJoinOn += !StringUtils.isBlank(table2Where) ? " AND " + table2Where : "";
						table2Where = "";
					}
				}
				
				String rsFrom = "FROM " + table1 + " " + table1Alias + " " 
						+ (StringUtils.isBlank(joinType) ? " "
								: join2Sql(joinType) + " " + table2 + " " + table2Alias + rsJoinOn) + " ";
				
				String rsWhere ="";
				if(!StringUtils.isBlank(table1Where)) {
					rsWhere += table1Where;
				}
				if(!StringUtils.isBlank(table2Where)) {
					rsWhere += (!StringUtils.isBlank(rsWhere) ? " AND " : "") + table2Where ;
				}
				if(!StringUtils.isBlank(rsWhere)) {
					rsWhere = "WHERE " + rsWhere;
				}
				
				rs = rsFrom + (!StringUtils.isBlank(rsWhere) ? "\n" + rsWhere : "");
				
				map.put("Step", step);
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
	 * 將Join寫法轉換成Sql寫法
	 * @param tableJoinCols
	 * @return
	 */
	private static String join2Sql(String tableJoinCols) {
		String rs = ">=".equals(tableJoinCols) ? "Left join"
				: "=".equals(tableJoinCols) ? "inner join"
						: "<=".equals(tableJoinCols) ? "right join"
								: "<=>".equals(tableJoinCols) ? "full outer join" : "";
		return rs;
	}
}
