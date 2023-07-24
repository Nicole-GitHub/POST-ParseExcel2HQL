package gss.TableLayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import gss.Tools.Tools;

public class ParseCols {
	private static final String className = ParseCols.class.getName();
	
	/**
	 * 取得 欄位處理邏輯 內容
	 * 
	 * @param path
	 * @param map
	 * @throws Exception 
	 */
	public static List<Map<String, String>> run (Sheet sheetCols, String partition) throws Exception {
		Row row = null;
		List<Map<String, String>> mapListCols = new ArrayList<Map<String, String>>();
		Map<Integer,String> mapOrder = new TreeMap<Integer,String>();
		List<Map<String, String>> rsSelectPartitionList = new ArrayList<Map<String, String>>();
		Map<String, String> mapPartition = new HashMap<String, String>();
		String targetTableOld = "", targetTableNew = "", rsSelect = "", rsGroup = "", rsCreateTable = "", rsTargetSelectCols = "", rsTmpTableSelectCols = "";
		boolean isPartition = false;
		
		try {

			// partition
			String[] partitionList = partition.split(",");
			
			// 解析資料內容(從第二ROW開頭爬)
			for (int r = 1; r <= sheetCols.getLastRowNum(); r++) {
				int c = 1; // 從第二CELL開頭爬
				row = sheetCols.getRow(r);
				
				c++; //來源
				String col = Tools.getCellValue(row, c++, "欄位處理邏輯");
				String group = Tools.getCellValue(row, c++, "群組");
				String order = Tools.getCellValue(row, c++, "排序欄位");
				String colAlias = Tools.getCellValue(row, c++, "欄位");
				String colDataType = Tools.getCellValue(row, c++, "格式");
				String targetTable = Tools.getCellValue(row, c++, "目的");
				targetTableNew = StringUtils.isBlank(targetTable) ? targetTableOld : targetTable;
				
				if(StringUtils.isBlank(targetTableOld)) 
					targetTableOld = targetTableNew;
				
				if (!targetTableOld.equals(targetTableNew)) {
					
					mapListCols.add(saveMapListCols(targetTableOld,  rsTmpTableSelectCols,  rsGroup, mapOrder, rsCreateTable));
					rsSelect = "";
					rsTmpTableSelectCols = "";
					rsGroup = "";
					rsCreateTable = "";
					mapOrder = new TreeMap<Integer,String>();
					targetTableOld = targetTableNew;
				}
				
				
				rsCreateTable += "\t" + colAlias + " " + colDataType + " ,\n";
				rsSelect = "\t" + col + " as " + colAlias + " ,\n";
				rsTmpTableSelectCols = rsSelect;
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

				// 組Target Table 的資訊時 Partiton 欄位的位置要另外放
				if("{raw}.TARGET".equalsIgnoreCase(targetTableNew)) {
					// Partiton欄位的位置要另外放
					isPartition = false;
					for (String str : partitionList) {
						if (colAlias.equals(str)) {
							mapPartition = new HashMap<String, String>();
							mapPartition.put("Col", colAlias);
							mapPartition.put("Script", rsSelect);
							rsSelectPartitionList.add(mapPartition);
							isPartition = true;
						}
					}

					// 非Partiton欄位的位置正常
					if(!isPartition) {
						rsTargetSelectCols += rsSelect;
					}
				}
			}

			rsTargetSelectCols += partitionList[0].length() > 0 ? Tools.tunePartitionOrder(partitionList, rsSelectPartitionList) : "";
			rsTargetSelectCols = rsTargetSelectCols.substring(0,rsTargetSelectCols.lastIndexOf(","));
			
			// Target Table 的 Create Script 用 Layout 頁籤產
			mapListCols.add(saveMapListCols(targetTableOld,  rsTargetSelectCols,  rsGroup, mapOrder, ""));
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapListCols;
	}
	
	/**
	 * 整理Cols欲存入Map的內容
	 * @param tableJoinCols
	 * @return
	 */
	private static Map<String, String> saveMapListCols(String targetTableOld, String rsSelect, String rsGroup,
			Map<Integer, String> mapOrder, String rsCreateTable) {
		Map<String, String> map = new HashMap<String, String>();
		String rsOrder = "", createSql = "";
		
		createSql = StringUtils.isBlank(rsCreateTable) ? "" 
				: "DROP TABLE IF EXISTS " + targetTableOld + " ;\n"
				+ "CREATE TABLE IF NOT EXISTS " + targetTableOld + " (\n"
				+ rsCreateTable.substring(0, rsCreateTable.length() - 2) + "\n);";
		
		map.put("Target", targetTableOld);
		map.put("CreateSql", createSql);
		map.put("Select", "Select \n" + rsSelect.substring(0,rsSelect.length() - 2));
		map.put("Group", StringUtils.isBlank(rsGroup) ? "" : "Group by " + rsGroup.substring(0,rsGroup.length() - 1));
		
		// order
		for(Entry<Integer,String> set : mapOrder.entrySet()) {
			rsOrder += set.getValue() + " ,";
		}
		map.put("Order", StringUtils.isBlank(rsOrder) ? "" : "Order by " + rsOrder.substring(0,rsOrder.length() - 1));

		return map;
	}
	
	
}
