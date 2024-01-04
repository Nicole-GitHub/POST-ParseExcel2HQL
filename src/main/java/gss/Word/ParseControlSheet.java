package gss.Word;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import gss.Tools.POITools;

/**
 * 解析POST第2階段ETL開發管控.xlsx內容
 */
public class ParseControlSheet {
	private static final String className = ParseControlSheet.class.getName();
	
	public static List<Map<String, String>> run(String fileNamePath, List<String> fileENameList) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
//		List<String> include = new ArrayList<String>();
		
		try {
		
			fileNamePath += "POST第2階段ETL開發管控.xlsx";
			System.out.println("\n\n=============================");
			System.out.println("fileName:" + fileNamePath);
			Workbook workbook = POITools.getWorkbook(fileNamePath);
			Sheet sheet = workbook.getSheet("派工進度");
			Row row;

			String targetTableEName = ""
				, targetTableEName_old = ""
				, targetTableCName = ""
				, sourceTableEName = ""
//				, sourceTableENameNoExt = ""
				, dataTransferInterval = ""
				, tdSourceTableENameArr = ""
				, sourceTableENameArr = ""
//				, sourceTableENameNoExtArr = ""
				;
			
			// 找出欲解析的資料有幾行
			for (int i = 1; i <= sheet.getLastRowNum(); i++) {
				int c = 0;
				row = sheet.getRow(i);
				
				if (row == null) {
					break;
				}

//				System.out.println(sheet.getLastRowNum() + "_" + i + ":" + row.getCell(0));
//				if(i == 269) {
//					System.out.println("Stop!");
//				}
				
				if("不處理".equals(POITools.getCellValue(row, 5, "狀態").toUpperCase()))
					continue;
					
				String targetTableEName_Temp = POITools.getCellValue(row, c++, "檔案名稱").toUpperCase();
				// 只整理這次要解析的table即可
				if (fileENameList.contains(targetTableEName_Temp)) {
					targetTableEName = targetTableEName_Temp;
//					if(!include.contains(targetTableEName)) {
//						include.add(targetTableEName);
					if(!targetTableEName_old.equals(targetTableEName)) {

						if(!StringUtils.isBlank(targetTableEName_old)) {
							map = new HashMap<String, String>();
							map.put("targetTableEName", targetTableEName_old);
							map.put("targetTableCName", targetTableCName);
							map.put("sourceTableENameArr", sourceTableENameArr);
//							map.put("sourceTableENameNoExtArr", sourceTableENameNoExtArr);
							map.put("dataTransferInterval", dataTransferInterval);
							map.put("tdSourceTableENameArr", tdSourceTableENameArr);
		
							mapList.add(map);
						
							targetTableCName = "";
							sourceTableEName = "";
//							sourceTableENameNoExt = "";
							sourceTableENameArr = "";
//							sourceTableENameNoExtArr = "";
							dataTransferInterval = "";
							tdSourceTableENameArr = "";
						}

						targetTableEName_old = targetTableEName;
						
						targetTableCName = POITools.getCellValue(row, c++, "中文檔案名稱").toUpperCase();
						c++; // 舊檔案名稱
						sourceTableEName = POITools.getCellValue(row, c++, "來源檔").toUpperCase();
//						sourceTableENameNoExt = sourceTableEName.indexOf(".") > 0
//								? sourceTableEName.substring(0, sourceTableEName.lastIndexOf("."))
//								: sourceTableEName;

						// 來源檔若為.ps則需更改副檔名為.txt
						if (sourceTableEName.toUpperCase().endsWith(".PS"))
							sourceTableEName = sourceTableEName.substring(0, sourceTableEName.lastIndexOf(".")) + ".txt";
						sourceTableENameArr += sourceTableEName + ",";
//						sourceTableENameNoExtArr += sourceTableENameNoExt + ",";
						
						c++; // 來源檔(公式)
						c++; // 狀態
						c++; // Owner
						c++; // 預計完成日
						c++; // 完成日
						c++; // 測試資料提供
						dataTransferInterval = StringUtils.isBlank(dataTransferInterval)
								? POITools.getCellValue(row, c++, "資料轉檔區間").toUpperCase()
								: dataTransferInterval;
						tdSourceTableENameArr += POITools.getCellValue(row, 18, "來源檔對應舊檔案名稱").toUpperCase() + ",";

					} else {
						targetTableCName = POITools.getCellValue(row, c++, "中文檔案名稱").toUpperCase();
						c++; // 舊檔案名稱
						sourceTableEName = POITools.getCellValue(row, c++, "來源檔").toUpperCase();
//						sourceTableENameNoExt = sourceTableEName.indexOf(".") > 0
//								? sourceTableEName.substring(0, sourceTableEName.lastIndexOf("."))
//								: sourceTableEName;

						// 來源檔若為.ps則需更改副檔名為.txt
						if (sourceTableEName.toUpperCase().endsWith(".PS"))
							sourceTableEName = sourceTableEName.substring(0, sourceTableEName.lastIndexOf(".")) + ".txt";
						sourceTableENameArr += sourceTableEName + ",";
//						sourceTableENameNoExtArr += sourceTableENameNoExt + ",";
						
						c++; // 來源檔(公式)
						c++; // 狀態
						c++; // Owner
						c++; // 預計完成日
						c++; // 完成日
						c++; // 測試資料提供
						dataTransferInterval = StringUtils.isBlank(dataTransferInterval)
								? POITools.getCellValue(row, c++, "資料轉檔區間").toUpperCase()
								: dataTransferInterval;
						tdSourceTableENameArr += POITools.getCellValue(row, 18, "來源檔對應舊檔案名稱").toUpperCase() + ",";

					}
				}
			}
			
			// 最後一筆資料需寫出
			{
				map = new HashMap<String, String>();
				map.put("targetTableEName", targetTableEName);
				map.put("targetTableCName", targetTableCName);
				map.put("sourceTableENameArr", sourceTableENameArr);
//				map.put("sourceTableENameNoExtArr", sourceTableENameNoExtArr);
				map.put("dataTransferInterval", dataTransferInterval);
				map.put("tdSourceTableENameArr", tdSourceTableENameArr);

				mapList.add(map);
			
				targetTableCName = "";
				sourceTableEName = "";
//				sourceTableENameNoExt = "";
				sourceTableENameArr = "";
//				sourceTableENameNoExtArr = "";
				dataTransferInterval = "";
				tdSourceTableENameArr = "";
			}
		
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapList;
	}

}
