package gss.Word;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import gss.Tools.Tools;

/**
 * 解析程式上版.xlsx的內容
 * @author nicole_tsou
 *
 */
public class RunParseTestInfo {
	private static final String className = RunParseTestInfo.class.getName();
	
	/**
	 * 取得 Excel 內容
	 * 
	 * @param tableLayoutPath
	 * @param map
	 * @throws Exception
	 */
	public static List<Map<String, String>> run() throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();

		try {
		
			String fileNamePath = "C:\\Users\\nicole_tsou\\Dropbox\\POST\\ETL\\ETL 第二階\\程式\\_TESTING\\"
					+ "程式上版.xlsx";
			System.out.println("\n\n=============================");
			System.out.println("fileName:" + fileNamePath);
			Workbook workbook = Tools.getWorkbook(fileNamePath);
//			Sheet sheetETL = workbook.getSheet("ETL");
			Sheet sheetTestList = workbook.getSheet("測試紀錄清單");
			Row row;
//			Cell cell;
//			int rowcount = 0;
			
			sheetTestList.getRow(0);

			// 找出欲解析的資料有幾行
			for (int i = 2; i <= sheetTestList.getLastRowNum(); i++) {
				int c = 0;
				row = sheetTestList.getRow(i);
				if (row == null) {
//					rowcount = i;
					break;
				}

				c++; //編號
				String testFileName = Tools.getCellValue(row, c++, "測試紀錄檔名").toUpperCase();
				String testNum = Tools.getCellValue(row, c++, "測試紀錄編號").toUpperCase();
				String targetTableEName = Tools.getCellValue(row, c++, "檔案名稱").toUpperCase();
				String targetTableCName = Tools.getCellValue(row, c++, "中文檔案名稱").toUpperCase();
				c++; //測試者
				c++; //測試類型
				String sourceTable = Tools.getCellValue(row, c++, "來源檔(含副檔名)").toUpperCase();
				String testDate = Tools.getCellValue(row, c++, "測試日期").toUpperCase();
				
				String type = sourceTable.startsWith("T_") == true ? "梳理" : "收載";
				
				// 來源檔若為.ps則需更改副檔名為.txt
				if(sourceTable.toUpperCase().endsWith(".PS"))
					sourceTable = sourceTable.substring(0,sourceTable.length() -3)+".txt";

				map = new HashMap<String, String>();
				map.put("_type", type);
				map.put("_testFileName", testFileName);
				
				map.put("[Replace_測試規格編號]", testNum);
				map.put("[Replace_測試規格名稱]", targetTableEName);
				map.put("[Replace_規格描述]", targetTableCName);
				map.put("[Replace_TargetTable]", targetTableEName);
				map.put("[Replace_ODSTable]", "ODS_"+targetTableEName.substring(2));
				map.put("[Replace_SourceTable]", sourceTable);
				map.put("[Replace_測試日期]", testDate);

				mapList.add(map);
			}
		
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapList;
	}

}
