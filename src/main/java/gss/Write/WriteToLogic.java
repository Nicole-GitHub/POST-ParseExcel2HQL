package gss.Write;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;

import gss.Tools.FileTools;
import gss.Tools.Tools;

/**
 * IFRS 17 自動產生邏輯HQL與頁籤與rcpt script
 * @author Nicole
 *
 */
public class WriteToLogic {
	private static final String className = WriteToLogic.class.getName();

	public static void run(String outputPath, String fileName, Workbook workbook,
			List<Map<String, String>> layoutMapList, String partition, Map<String, String> mapProp) throws Exception {
		Sheet sheet1 = null, sheet2 = null;
		Row rowSheet1 = null, rowSheet2 = null;
		Cell cell = null;
		
		try {

	     	int r = 2, cSheet1 = 0;
	     	String colLogic = "", numColLogic = "", rcptColLogic = "", rcptODSColLogic = "", rsTargetSelectCols = "";
	     	boolean isPartition = false;

	     	HashMap<String, String> mapPartition = new HashMap<String, String>();
	     	List<Map<String, String>> rsSelectPartitionList = new ArrayList<Map<String, String>>();

	     	List<String> charTypeList = Arrays.asList(new String[] { "VARCHAR", "CHAR" });
			List<String> intTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER" });
	        CellStyle style = Tools.setTitleStyle(workbook);
	        
			if ("1".equals(mapProp.get("runType"))) {
				sheet1 = workbook.createSheet("資料關聯");
				sheet2 = workbook.createSheet("欄位處理邏輯");
			}
			
			// partition
			String[] partitionList = partition.split(",");

			if ("1".equals(mapProp.get("runType"))) {
				// 設定標題 & 凍結首欄 & 首欄篩選
				Tools.setTitle(sheet1, "K", style,
						Arrays.asList("步驟", "目的", "資料表", "別名", "JOIN欄位", "條件", "關聯", "資料表", "別名", "JOIN欄位", "條件"));
				Tools.setTitle(sheet2, "H", style, Arrays.asList("步驟", "來源", "欄位處理邏輯", "群組", "排序欄位", "欄位", "格式", "目的"));
			}
	        
	     	// list的最後一筆位置
			int layoutMapListLastNum = layoutMapList.size() - 1;
			String tableName = layoutMapList.get(layoutMapListLastNum).get("TableName");
	     	String odsTableName = "ODS" + tableName.substring(1);
			String tableType = "D" + tableName.substring(5, 6);

			if ("1".equals(mapProp.get("runType"))) {
				// 設定內容
				style = Tools.setStyle(workbook);
				rowSheet1 = sheet1.createRow(1);
				Tools.setStringCell(style, cell, rowSheet1, cSheet1++, "L01");
				Tools.setStringCell(style, cell, rowSheet1, cSheet1++, "{raw}.TARGET");
				Tools.setStringCell(style, cell, rowSheet1, cSheet1++, "{raw}." + odsTableName.toUpperCase());
				Tools.setStringCell(style, cell, rowSheet1, cSheet1++, "T1");

				rowSheet2 = sheet2.createRow(1);
				/**
				 * 合併儲存格
				 */
				// 合併行數 = list的最後一筆位置(因list的最後一筆非欄位資訊故-1)
				int mergeNum = layoutMapListLastNum;
				CellRangeAddress cra0 = new CellRangeAddress(1, mergeNum, 0, 0); // 起始行, 終止行, 起始列, 終止列
				CellRangeAddress cra1 = new CellRangeAddress(1, mergeNum, 1, 1);
				CellRangeAddress cra3 = new CellRangeAddress(1, mergeNum, 3, 3);
				CellRangeAddress cra4 = new CellRangeAddress(1, mergeNum, 4, 4);
				CellRangeAddress cra6 = new CellRangeAddress(1, mergeNum, 6, 6);
				CellRangeAddress cra7 = new CellRangeAddress(1, mergeNum, 7, 7);
				sheet2.addMergedRegion(cra0);
				sheet2.addMergedRegion(cra1);
				sheet2.addMergedRegion(cra3);
				sheet2.addMergedRegion(cra4);
				sheet2.addMergedRegion(cra6);
				sheet2.addMergedRegion(cra7);
//			// 使用RegionUtil類為合併後的單元格添加邊框
//			RegionUtil.setBorderBottom(1, cra1, sheet2, workbook); // 下邊框
//			RegionUtil.setBorderLeft(1, cra1, sheet2, workbook); // 左邊框
//			RegionUtil.setBorderRight(1, cra1, sheet2, workbook); // 有邊框
//			RegionUtil.setBorderTop(1, cra1, sheet2, workbook); // 上邊框

				Tools.setStringCell(style, cell, rowSheet2, 0, "L01");
				Tools.setStringCell(style, cell, rowSheet2, 1, "{raw}." + odsTableName.toLowerCase());
				Tools.setStringCell(style, cell, rowSheet2, 3, "");
				Tools.setStringCell(style, cell, rowSheet2, 4, "");
				Tools.setStringCell(style, cell, rowSheet2, 6, "");
				Tools.setStringCell(style, cell, rowSheet2, 7, "{raw}.TARGET");
			}
	        
			for (Map<String, String> layoutMap : layoutMapList) {
				if ("Detail".equals(layoutMap.get("MapType"))) {
					String colEName = layoutMap.get("ColEName").toString().toUpperCase();
					String colType = layoutMap.get("ColType").toString().toUpperCase();
					
					if (charTypeList.contains(colType))
						colLogic = colEName;
					else if (intTypeList.contains(colType)) {
						colLogic = "cast(" + colEName + " as " + colType + ")";
						numColLogic = colLogic;
					} else if ("DATE".equals(colType))
						colLogic = "case when " + colEName
								+ " = '00000000' then NULL else to_date(from_unixtime(unix_timestamp(" + colEName
								+ ", 'yyyyMMdd'))) end";
					else if ("DECIMAL".equals(colType)) {
						colLogic = "cast(" + colEName + " as " + colType + "(" + layoutMap.get("ColLen") + "))";
						numColLogic = colLogic;
					} else if ("DATETIME".equals(colType))
						colLogic = "current_timestamp";

					if ("1".equals(mapProp.get("runType"))) {
						// 寫入Excel
						Tools.setStringCell(style, cell, rowSheet2, 2, colLogic);
						Tools.setStringCell(style, cell, rowSheet2, 5, colEName);
						// 因第二行已在Merge時create過了故此行寫在最後
						rowSheet2 = sheet2.createRow(r++);
					}
					
					// 寫入HQL
					// Partiton欄位的位置要另外放
					colLogic = "\t" + colLogic + " as " + colEName + " ,\n";
					isPartition = false;
					for (String str : partitionList) {
						if (colEName.equals(str)) {
							mapPartition = new HashMap<String, String>();
							mapPartition.put("Col", colEName);
							mapPartition.put("Script", colLogic);
							rsSelectPartitionList.add(mapPartition);
							isPartition = true;
						}
					}

					// 非Partiton欄位的位置正常
					if (!isPartition) {
						rsTargetSelectCols += colLogic;
						if (intTypeList.contains(colType)) {
							rcptODSColLogic += "\tsum(" + numColLogic + ") as " + colEName + " ,\n";
							rcptColLogic += "\tsum(" + colEName + ") as " + colEName + " ,\n";
						} else if ("DECIMAL".equals(colType)) {
							rcptODSColLogic += "\tsum(" + numColLogic + ") as " + colEName + " ,\n";
							rcptColLogic += "\tsum(" + colEName + ") as " + colEName + " ,\n";
						}
					}
				}
			}

			outputPath += fileName + "/";
			if ("1".equals(mapProp.get("runType"))) {
				rsTargetSelectCols += Tools.tunePartitionOrder(partitionList, rsSelectPartitionList);

				// 組出完整hql
				String rawDBName = mapProp.get("hadoop.raw.dbname");
				String sql = "INSERT OVERWRITE TABLE " + rawDBName + "." + tableName + " \n" + "PARTITION("
						+ (StringUtils.isBlank(partition) ? "" : partition + ",") + " batchid) \n" + "Select \n"
						+ rsTargetSelectCols + "FROM " + rawDBName + "." + odsTableName + " T1 ;";
				// 將整理好的比對結果另寫出Excel檔
				Tools.output(workbook, outputPath, fileName);
				FileTools.createFile(outputPath, tableType + "_L01", "hql", sql);
			}
			
			// 產出測試時所需SQL
			WriteToRCPT.run(outputPath, mapProp, tableName, odsTableName, rcptODSColLogic, rcptColLogic);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}

}
