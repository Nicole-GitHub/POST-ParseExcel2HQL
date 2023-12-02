package gss.Word;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

		try {
		
			fileNamePath += "POST第2階段ETL開發管控.xlsx";
			System.out.println("\n\n=============================");
			System.out.println("fileName:" + fileNamePath);
			Workbook workbook = POITools.getWorkbook(fileNamePath);
			Sheet sheet = workbook.getSheet("派工進度");
			Row row;
			
			// 找出欲解析的資料有幾行
			for (int i = 2; i <= sheet.getLastRowNum(); i++) {
				int c = 0;
				row = sheet.getRow(i);
				if (row == null) {
					break;
				}

				String targetTableEName = POITools.getCellValue(row, c++, "檔案名稱").toUpperCase();
				// 只整理這次要解析的table即可
				if (fileENameList.contains(targetTableEName)) {
					
					String targetTableCName = POITools.getCellValue(row, c++, "中文檔案名稱").toUpperCase();
					c++; // 舊檔案名稱
					String sourceTableEName = POITools.getCellValue(row, c++, "來源檔").toUpperCase();
					String sourceTableENameNoExt = sourceTableEName.substring(0, sourceTableEName.lastIndexOf("."));
					c++; // 來源檔(公式)
					c++; // 狀態
					c++; // Owner
					c++; // 預計完成日
					c++; // 完成日
					c++; // 測試資料提供
					String dataTransferInterval = POITools.getCellValue(row, c++, "資料轉檔區間").toUpperCase();

					// 來源檔若為.ps則需更改副檔名為.txt
					if (sourceTableEName.toUpperCase().endsWith(".PS"))
						sourceTableEName = sourceTableEName.substring(0, sourceTableEName.length() - 3) + ".txt";

					map = new HashMap<String, String>();
					map.put("targetTableEName", targetTableEName);
					map.put("targetTableCName", targetTableCName);
					map.put("sourceTableEName", sourceTableEName);
					map.put("sourceTableENameNoExt", sourceTableENameNoExt);
					map.put("dataTransferInterval", dataTransferInterval);

					mapList.add(map);
				}
			}
		
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapList;
	}

}
