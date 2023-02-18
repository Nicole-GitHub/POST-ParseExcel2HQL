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

	public static void run(String outputPath, String tableLayoutPath, String fileName, List<Map<String, String>> layoutMapList,
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
			
			// 整理出Layout頁籤數值型態的欄位
			List<String> listNumTypeColName = new ArrayList<String>();
			List<Map<String, String>> listNumType = new ArrayList<Map<String, String>>();
			List<String> numTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER", "DECIMAL" });
			for (Map<String, String> layoutMap : layoutMapList) {
				if ("Detail".equals(layoutMap.get("MapType"))) {
					String colType = layoutMap.get("ColType").toString().toUpperCase();
					if (numTypeList.contains(colType)) {
						listNumType.add(layoutMap);
						listNumTypeColName.add(layoutMap.get("ColEName"));
					}
				}
			}
			
			
	        XSSFWorkbook workbook = new XSSFWorkbook();
	        XSSFSheet sheet1 = workbook.createSheet("Full Content");
	        XSSFSheet sheet2 = workbook.createSheet("Number Type Content");
	        XSSFSheet sheet3 = workbook.createSheet("Number Type Summary");

	        CellStyle style = Tools.setTitleStyle(workbook);
	        CellStyle styleNum = null;
	        String lastExcelColNameSheet1 = Tools.getLastExcelColName(dataColsList.length);
	        String lastExcelColNameSheet2 = Tools.getLastExcelColName(listNumTypeColName.size());

	     	int r = 1, cSheet1 = 0, cSheet3 = 0, line = 0;
	     	Double[] sumNumCol = new Double[dataColsList.length];
	     	Integer[] decimalPlacesCol = new Integer[dataColsList.length];
	     	
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
						for(int j = 0 ; j < listNumType.size() ; j++) {
							int decimalPlaces = 0;
							if (odsColsName.equalsIgnoreCase(listNumType.get(j).get("ColEName"))) {
								String numContentStr = content.substring(start, end).trim();
								numContentStr = StringUtils.isBlank(numContentStr) ? "0" : numContentStr;
								Double numContentDouble = Double.parseDouble(numContentStr);
								if("DECIMAL".equalsIgnoreCase(listNumType.get(j).get("ColType"))) {
									decimalPlaces = Integer.parseInt(listNumType.get(j).get("ColLen").split(",")[1]);
									styleNum = Tools.setNumStyle(workbook,decimalPlaces);
									Tools.setNumericCell(styleNum, cell, rowSheet2, j, numContentDouble);
								}else {
									Tools.setNumericCell(style, cell, rowSheet2, j, numContentDouble);
								}

								// 將數值欄位的值加總
								sumNumCol[j] = sumNumCol[j] == null ? 0 : sumNumCol[j];
								sumNumCol[j] += numContentDouble;

								// 將數值欄位的小數位數存入
								decimalPlacesCol[j] = decimalPlacesCol[j] == null ? decimalPlaces : decimalPlacesCol[j];

								break;
							}
						}
					} else
						break;
				}
			}

			// 將加總後的數值寫入sheet3
			cSheet3 = 0;
			rowSheet3 = sheet3.createRow(1);
			for(int i = 0 ; i < sumNumCol.length ; i++) {
				if(sumNumCol[i] != null) {
			        styleNum = Tools.setNumStyle(workbook, decimalPlacesCol[i]);
					Tools.setNumericCell(styleNum, cell, rowSheet3, cSheet3++, sumNumCol[i]);
				}
			}

			// 將整理好的比對結果另寫出Excel檔
			Tools.output(workbook, outputPath + fileName + "/", sourceFileName + "_" + fileName);
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
}
