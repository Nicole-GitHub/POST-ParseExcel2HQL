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

import gss.ETLCode.bin.DW_L07_LoadDW;
import gss.Tools.FileTools;
import gss.Tools.POITools;
import gss.Tools.Tools;

/**
 * 
 * @author Nicole
 *
 */
public class WriteToLogic {
	private static final String className = WriteToLogic.class.getName();

	/**
	 * 產生邏輯HQL與"資料關聯"、"欄位處理邏輯"頁籤
	 * 
	 * @param outputPath
	 * @param fileName
	 * @param workbook
	 * @param layoutMapList
	 * @param partition
	 * @param mapProp
	 * @throws Exception
	 */
	public static void run(String outputPath, String fileName, Workbook workbook,
			List<Map<String, String>> layoutMapList, Map<String, String> mapProp) throws Exception {
		Sheet sheet1 = null, sheet2 = null;
		Row rowSheet1 = null, rowSheet2 = null;
		Cell cell = null;
		Map<String, String> layoutMap = new HashMap<String, String>();
		
		try {

	     	int r = 2, cSheet1 = 0;
	     	String colLogic = "", rsTargetSelectCols = "";
	     	boolean isPartition = false;

	     	HashMap<String, String> mapPartition = new HashMap<String, String>();
	     	List<Map<String, String>> rsSelectPartitionList = new ArrayList<Map<String, String>>();

	     	List<String> charTypeList = Arrays.asList(new String[] { "VARCHAR", "CHAR" });
			List<String> intTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER", "INT" });
	        CellStyle style = POITools.setTitleStyle(workbook);
	        
			sheet1 = workbook.createSheet("資料關聯");
			sheet2 = workbook.createSheet("欄位處理邏輯");
			

			// 設定標題 & 凍結首欄 & 首欄篩選
			POITools.setTitle(sheet1, "K", style,
					Arrays.asList("步驟", "目的", "資料表", "別名", "JOIN欄位", "條件", "關聯", "資料表", "別名", "JOIN欄位", "條件"));
			POITools.setTitle(sheet2, "H", style, Arrays.asList("步驟", "來源", "欄位處理邏輯", "群組", "排序欄位", "欄位", "格式", "目的"));
	        
	     	// list的最後一筆位置
			int layoutMapListLastNum = layoutMapList.size() - 1;
			layoutMap = layoutMapList.get(layoutMapListLastNum);
			String tableName = layoutMap.get("TableName");
	     	String odsTableName = "ODS" + tableName.substring(1);
			String partition = layoutMap.get("Partition");
			
			// partition
			String[] partitionList = partition.split(",");
			
			// 設定內容
			style = POITools.setStyle(workbook);
			rowSheet1 = sheet1.createRow(1);
			POITools.setStringCell(style, cell, rowSheet1, cSheet1++, "L01");
			POITools.setStringCell(style, cell, rowSheet1, cSheet1++, "{raw}.TARGET");
			POITools.setStringCell(style, cell, rowSheet1, cSheet1++, "{raw}." + odsTableName.toUpperCase());
			POITools.setStringCell(style, cell, rowSheet1, cSheet1++, "T1");

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

			POITools.setStringCell(style, cell, rowSheet2, 0, "L01");
			POITools.setStringCell(style, cell, rowSheet2, 1, "{raw}." + odsTableName.toLowerCase());
			POITools.setStringCell(style, cell, rowSheet2, 3, "");
			POITools.setStringCell(style, cell, rowSheet2, 4, "");
			POITools.setStringCell(style, cell, rowSheet2, 6, "");
			POITools.setStringCell(style, cell, rowSheet2, 7, "{raw}.TARGET");
			
			String sumODSColLogic = "", sumColLogic = "", whereSumCol = "";
			for (Map<String, String> layoutMapFor : layoutMapList) {
				if ("Detail".equals(layoutMapFor.get("MapType"))) {
					String colEName = layoutMapFor.get("ColEName").toString().toUpperCase();
					String colType = layoutMapFor.get("ColType").toString().toUpperCase();
					String colLen = layoutMapFor.get("ColLen").toString().toUpperCase();
					String formular = layoutMapFor.get("Formular").toString();

					colLogic = colEName;
					// 欄位型態轉換(收載時才需做轉換)
					if ("1".equals(mapProp.get("runType")))
						colLogic = getColLogic( charTypeList, intTypeList, colEName, colType, colLen);

					if ("DATETIME".equalsIgnoreCase(colType) 
							&& ("CREATEDDATE".equalsIgnoreCase(colEName)
									|| "MODIFIEDDATE".equalsIgnoreCase(colEName)))
						colLogic = "current_timestamp";

					if ("CREATEDUSER".equalsIgnoreCase(colEName) || "MODIFIEDUSER".equalsIgnoreCase(colEName))
						colLogic = "'ETL'";
					
					if (intTypeList.contains(colType) || "DECIMAL".equals(colType)) {
						sumColLogic += "\t\t\tnvl(sum(" + colEName + "),0) as " + colEName + " ,\n";
						sumODSColLogic += "\t\t\tnvl(sum(" + colLogic + "),0) as SRC_" + colEName + " ,\n";
						whereSumCol += "\t\tand a." + colEName + " = b.SRC_" + colEName + "\n";
					}
					
					// 寫入Excel
					POITools.setStringCell(style, cell, rowSheet2, 2, colLogic);
					POITools.setStringCell(style, cell, rowSheet2, 5, colEName);
					// 因第二行已在Merge時create過了故此行寫在最後
					rowSheet2 = sheet2.createRow(r++);

					// 寫入HQL
					if (!StringUtils.isBlank(formular))
						colLogic = "\t" + formular + " as " + colEName + " ,\n";
					else
						colLogic = "\t"
								+ ("current_timestamp".equals(colLogic) 
										|| "1".equals(mapProp.get("runType"))
										|| "'ETL'".equals(colLogic)
									? "" : "T1.")
								+ colLogic + " as " + colEName + " ,\n";
					
					// Partiton欄位的位置要另外放
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
					if (!isPartition) 
						rsTargetSelectCols += colLogic;
				}
			}

			outputPath += fileName + "/";
			// 調整最後輸出的partition順序(需與Layout頁籤的partition欄位相同)
			rsTargetSelectCols += partitionList[0].length() > 0 ? Tools.tunePartitionOrder(partitionList, rsSelectPartitionList) : "";
			rsTargetSelectCols = rsTargetSelectCols.substring(0,rsTargetSelectCols.lastIndexOf(","));
			
			sumColLogic = StringUtils.isBlank(sumColLogic) ? "" : sumColLogic.substring(0,sumColLogic.length() - 2);
			sumODSColLogic = StringUtils.isBlank(sumODSColLogic) ? "" : sumODSColLogic.substring(0,sumODSColLogic.length() - 2);
			whereSumCol = StringUtils.isBlank(whereSumCol) ? "" : whereSumCol.substring(6);

			// 組出完整hql (DW_L07_LoadDW)
//			String rsHQL = DW_L07_LoadDW.getHQL(partition, mapProp, rsTargetSelectCols, sumColLogic, sumODSColLogic,
//					whereSumCol, tableName, odsTableName);
//			String rsVAR = DW_L07_LoadDW.getVAR(mapProp, tableName, odsTableName);
			
			// 將整理好的比對結果另寫出Excel檔
			// 收載才需產出Excel的邏輯頁籤，梳理則不需
			if("1".equals(mapProp.get("runType"))) {
				// "資料關聯"、"欄位處理邏輯"頁籤用不到
				// 故不另外產出Layout檔，以免與原Layout檔搞混
//				POITools.output(workbook, outputPath, fileName);
			} else {
				workbook.close();
			}

			String type = "D"+tableName.substring(5,6);
			outputPath += "bin/";
			String codeFileName = "1".equals(mapProp.get("runType")) ? "DW_L07_LoadDW" : type+"_T01" ;
//			FileTools.createFileNotAppend(outputPath, codeFileName, "hql", rsHQL);
//			FileTools.createFileNotAppend(outputPath, codeFileName, "var", rsVAR);
			
			/******************
			 * airflow
			 ******************/
			String rsHQL_airflow = DW_L07_LoadDW.getHQL_airflow(partition, mapProp, rsTargetSelectCols, sumColLogic, sumODSColLogic,
					whereSumCol, tableName, odsTableName, type);
			FileTools.createFileNotAppend(outputPath, codeFileName, "hql", rsHQL_airflow);

		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}

	/**
	 * 欄位型態轉換
	 * @param charTypeList
	 * @param intTypeList
	 * @param colEName
	 * @param colType
	 * @param colLen
	 * @return
	 * @throws Exception
	 */
	public static String getColLogic(List<String> charTypeList, List<String> intTypeList, String colEName,
			String colType, String colLen) throws Exception {
		
		String colLogic = "";
		
		if (charTypeList.contains(colType))
			colLogic = colEName;
		else if (intTypeList.contains(colType))
			colLogic = "cast(" + colEName + " as " + colType + ")";
		else if ("DATE".equals(colType))
			colLogic = "case when " + colEName
					+ " = '00000000' then NULL else to_date(from_unixtime(unix_timestamp(" + colEName
					+ ", 'yyyyMMdd'))) end";
		else if ("DECIMAL".equals(colType)) 
			colLogic = "cast(" + colEName + " as " + colType + "(" + colLen + "))";
//		else if ("DATETIME".equalsIgnoreCase(colType) && "CREATEDDATE".equalsIgnoreCase(colEName))
//			colLogic = "current_timestamp";
		
		return colLogic;
	}

}
