package gss.SourceFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import gss.Tools.FileTools;
import gss.Tools.Tools;

/**
 * 將文字檔轉成Excel
 * @author Nicole
 *
 */
public class RunParseSourceFile {
	private static final String className = RunParseSourceFile.class.getName();

	public static void run(String tableLayoutPath, String fileName, List<Map<String, String>> layoutMapList,
			Map<String, String> odsMap) throws Exception {
		
		Row rowSheet1 = null, rowSheet2 = null, rowSheet3 = null;
		Cell cell = null;
		
		try {
			String sourceFileName = odsMap.get("SourceFileName").toString();
			String dataStartEnd = odsMap.get("DataStartEnd").toString();
			String dataCols = odsMap.get("DataCols").toString();
			
			String sourceFilePath = tableLayoutPath + "SourceFile/";
			System.out.println("fileName:" + sourceFilePath + sourceFileName + ".txt");
			String sourceFileContent = FileTools.readFileContent(sourceFilePath + sourceFileName + ".txt");

			String[] sourceFileContentList = sourceFileContent.split("\n");
			String[] dataStartEndList = dataStartEnd.split(",");
			String[] dataColsList = dataCols.split(",");
			
			List<String> listNumTypeColName = listNumTypeColName(layoutMapList);
			
	        XSSFWorkbook workbook = new XSSFWorkbook();
	        XSSFSheet sheet1 = workbook.createSheet("Full Content");
	        XSSFSheet sheet2 = workbook.createSheet("Number Type Content");
	        XSSFSheet sheet3 = workbook.createSheet("Number Type Summary");

	        CellStyle style = Tools.setTitleStyle(workbook);
	        String lastExcelColNameSheet1 = Tools.getLastExcelColName(dataColsList.length);
	        String lastExcelColNameSheet2 = Tools.getLastExcelColName(listNumTypeColName.size());

	     	int r = 1, cSheet1 = 0, cSheet3 = 0, line = 0;
	     	Double[] sumNumCol = new Double[dataColsList.length];
	     	
	        // 設定標題 & 凍結首欄 & 首欄篩選
			Tools.setTitle(sheet1, lastExcelColNameSheet1, style, Arrays.asList(dataColsList));
			Tools.setTitle(sheet2, lastExcelColNameSheet2, style, listNumTypeColName);
			Tools.setTitle(sheet3, lastExcelColNameSheet2, style, listNumTypeColName);
			
			// 設定內容
			style = Tools.setStyle(workbook);
			for (String content : sourceFileContentList) {
				// 去除表頭表尾
				line ++;
				if(line == 1 || line == sourceFileContentList.length)
					continue;
				
				cSheet1 = 0;
				rowSheet1 = sheet1.createRow(r);
				rowSheet2 = sheet2.createRow(r);
				r++;
				for (int i = 0; i < dataStartEndList.length;) {
					int start = Integer.parseInt(dataStartEndList[i++]) - 1;
					int end = Integer.parseInt(dataStartEndList[i++]);
					int dataColsListNum = i / 2 - 1;
					String odsColsName = dataColsList[dataColsListNum];
					// 若文字檔內容長度 >= 資料終止位置 則寫入 sheet1
					if (content.length() >= end) {
						Tools.setStringCell(style, cell, rowSheet1, cSheet1++, content.substring(start, end));
						// 若此欄為數值型態欄位則也寫入一份至sheet2
						for(int j = 0 ; j < listNumTypeColName.size() ; j++) {
							if (odsColsName.equalsIgnoreCase(listNumTypeColName.get(j).toString())) {
								String numContentStr = content.substring(start, end).trim();
								numContentStr = StringUtils.isBlank(numContentStr) ? "0" : numContentStr;
								Double numContentDouble = Double.parseDouble(numContentStr);
								Tools.setNumericCell(style, cell, rowSheet2, j, numContentDouble);

								// 將數值欄位的值加總
								sumNumCol[j] = sumNumCol[j] == null ? 0 : sumNumCol[j];
								sumNumCol[j] += numContentDouble;
								
								break;
							}
						}
					} else
						break;
				}
			}

			// 將加總後的數值寫入sheet3
			List<Double> sumNumColList = new ArrayList<Double>();
			cSheet3 = 0;
			rowSheet3 = sheet3.createRow(1);
			for(Double sumNum : sumNumCol) {
				if(sumNum != null)
					sumNumColList.add(sumNum);
			}

			for(Double sumNum : sumNumColList) {
				Tools.setNumericCell(style, cell, rowSheet3, cSheet3++, sumNum);
			}
			
			// 將整理好的比對結果另寫出Excel檔
			Tools.output(workbook, tableLayoutPath + "../Output/" + fileName + "/", sourceFileName + "_" + fileName);
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
	

	/**
	 * 列出Layout頁籤數值型態的欄位名稱
	 * @param layoutSheet
	 * @return
	 * @throws Exception
	 */
	public static List<String> listNumTypeColName(List<Map<String, String>> layoutMapList) throws Exception {
		List<String> list = new ArrayList<String>();
		List<String> numTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER", "DECIMAL" });

		for (Map<String, String> layoutMap : layoutMapList) {
			if ("Detail".equals(layoutMap.get("MapType"))) {
				if (numTypeList.contains(layoutMap.get("ColType").toString().toUpperCase())) {
					list.add(layoutMap.get("ColEName").toString());
				}
			}
		}
		return list;
	}
}
