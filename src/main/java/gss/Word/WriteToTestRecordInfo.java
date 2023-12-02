package gss.Word;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import gss.Tools.POITools;
import gss.Tools.Tools;

/**
 * 匯出程式上版.xlsx 
 * 並拋出對應的測試紀錄編號
 * 
 * @author nicole_tsou
 *
 */
public class WriteToTestRecordInfo {
	private static final String className = WriteToTestRecordInfo.class.getName();

	/**
	 * @param testRecordMap    整理測試紀錄各項目的編號最大值
	 * @param controlSheetList 解析POST第2階段ETL開發管控.xlsx內容
	 * @throws Exception
	 */
	public static Map<String, String> run(String fileNamePath, String outputPath, Map<String, Integer> testRecordMap,
			List<Map<String, String>> controlSheetList) throws Exception {
//		String fileNamePath = "C:\\Users\\nicole_tsou\\Dropbox\\POST\\JavaTools\\POST-ParseExcel2HQL\\";
		System.out.println("\n\n=============================");
		System.out.println("fileName:" + fileNamePath);

		Map<String, String> testRecordNumMap= new HashMap<String, String>();
		
		Workbook workbook = POITools.getWorkbook(fileNamePath + "Sample-程式上版.xlsx");
		Sheet sheet = workbook.getSheet("測試紀錄清單");
		CellStyle style = POITools.setTitleStyle(workbook);
		Row row = null;
		Cell cell = null;
		
		try {
			
			// 設定內容
			style = POITools.setStyle(workbook);
			
			for(int i = 0 ; i < controlSheetList.size() ; i++) {

				row = sheet.createRow(i+1); //從第二行開始
				String targetTableEName = controlSheetList.get(i).get("targetTableEName");
				String targetTableCName = controlSheetList.get(i).get("targetTableCName");
				
				// 測試紀錄項目
				String testRecordType = "ETL_" + targetTableEName.substring(2, 5) + "_"
						+ targetTableEName.substring(7, 9);
				// 若查無項目則編號給1，若有則編號+1
				int testRecordNum = testRecordMap.get(testRecordType) == null ? 1
						: Integer.parseInt(testRecordMap.get(testRecordType).toString()) + 1;
				// 編號兩碼，不足左邊補0
				String testRecordNumStr = testRecordType + "_" + String.format("%02d", testRecordNum);
				
				// 更新測試紀錄項目編號
				if(testRecordMap.get(testRecordType) == null 
						|| Integer.parseInt(testRecordMap.get(testRecordType).toString()) < testRecordNum)
					testRecordMap.put(testRecordType, testRecordNum);
				
				
				POITools.setStringCell(style, cell, row, 0, testRecordNumStr); // 測試紀錄編號
				POITools.setStringCell(style, cell, row, 1, targetTableEName); // 檔案名稱
				POITools.setStringCell(style, cell, row, 2, targetTableCName); // 中文檔案名稱
				POITools.setStringCell(style, cell, row, 3, "鄒文瑩"); // 測試者
				POITools.setStringCell(style, cell, row, 4, "單元測試"); // 測試類型
				
				// 整理出對應的測試紀錄編號並拋出
				testRecordNumMap.put(targetTableEName, testRecordNumStr);
			}

			// 寫出整理好的Excel
			POITools.output(workbook, outputPath, "程式上版" + Tools.getNOW("yyyyMMdd"));
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return testRecordNumMap;
	}

}
