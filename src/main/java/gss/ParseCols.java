package gss;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class ParseCols {
	private static final String className = ParseCols.class.getName();
	
	/**
	 * 取得 欄位處理邏輯 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception 
	 */
	public static List<Map<String, String>> run (Sheet sheetCols) throws Exception {
		Row row = null;
		List<Map<String, String>> mapListCols = new ArrayList<Map<String, String>>();

		try {
			// 解析資料內容(從第二ROW開頭爬)
			String targetColsOld = "", targetColsNew = "", rsSelect = "", rsGroup = "";
			Map<Integer,String> mapOrder = new TreeMap<Integer,String>();
			for (int r = 1; r <= sheetCols.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetCols.getRow(r);
				
				c++; //來源
				String col = Tools.getCellValue(row, c++, "欄位處理邏輯");
				String group = Tools.getCellValue(row, c++, "群組");
				String order = Tools.getCellValue(row, c++, "排序欄位");
				String colAlias = Tools.getCellValue(row, c++, "欄位");
				c += 2;
				String targetCols = Tools.getCellValue(row, c++, "目的");
				targetColsNew = StringUtils.isBlank(targetCols) ? targetColsOld : targetCols;
				
				if(StringUtils.isBlank(targetColsOld)) 
					targetColsOld = targetColsNew;
				
				if (!targetColsOld.equals(targetColsNew)) {
					
					mapListCols.add(saveMapListCols(targetColsOld,  rsSelect,  rsGroup, mapOrder));
					rsSelect = "";
					rsGroup = "";
					mapOrder = new TreeMap<Integer,String>();
					targetColsOld = targetColsNew;
				}
				
				rsSelect += col + " as " + colAlias + " ,";
				rsGroup += "Y".equals(group) ? col + " ," : "";
				// order
				if(!StringUtils.isBlank(order)) {
					order =  col + " "+order;
					if(order.contains(",")) {
						String[] orderArr = order.split(",");
						mapOrder.put(Integer.parseInt(orderArr[1]), orderArr[0]);
					}else {
						mapOrder.put(r, order);
					}
				}
			}
			
			mapListCols.add(saveMapListCols(targetColsOld,  rsSelect,  rsGroup, mapOrder));
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

System.out.println("ParseCols Done!");
		return mapListCols;
	}
	
	/**
	 * 整理Cols欲存入Map的內容
	 * @param tableJoinCols
	 * @return
	 */
	private static Map<String, String> saveMapListCols(String targetColsOld,String rsSelect,String rsGroup,Map<Integer, String> mapOrder) {
		Map<String, String> map = new HashMap<String, String>();
		String rsOrder = "";
		map.put("Target", targetColsOld);
		map.put("Select", "Select " + rsSelect.substring(0,rsSelect.length() - 1));
		map.put("Group", StringUtils.isBlank(rsGroup) ? "" : "Group by " + rsGroup.substring(0,rsGroup.length() - 1));
		// order
		for(Entry<Integer,String> set : mapOrder.entrySet()) {
			rsOrder += set.getValue() + " ,";
		}
		map.put("Order", StringUtils.isBlank(rsOrder) ? "" : "Order by " + rsOrder.substring(0,rsOrder.length() - 1));

		return map;
	}
	
	
}
