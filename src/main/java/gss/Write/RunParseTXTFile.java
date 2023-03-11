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
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import gss.TableLayout.ParseLayout;
import gss.Tools.FileTools;
import gss.Tools.Tools;

public class RunParseTXTFile {
	private static final String className = RunParseTXTFile.class.getName();
	
	public static void parseSourceFile(String tableLayoutPath, String fileName, List<Map<String, String>> layoutMapList,
			Map<String, String> odsMap) throws Exception {

		String funcName = "parseSourceFile";
		try {
			String txtFileName = odsMap.get("TXTFileName").toString();
			String dataStartEnd = odsMap.get("DataStartEnd").toString();
			String dataCols = odsMap.get("DataCols").toString();
			
			String[] dataStartEndList = dataStartEnd.split(",");
			String[] dataColsList = dataCols.split(",");

			toExcel(fileName, tableLayoutPath, txtFileName, layoutMapList, Arrays.asList(dataColsList),
					Arrays.asList(dataStartEndList));
		} catch (Exception ex) {
			throw new Exception(className + " " + funcName + " Error: \n" + ex);
		}

		System.out.println(className + " " + funcName + " Done!");
	}
	
	public static void parseExportFile(Map<String, String> mapProp, String tableLayoutPath, List<String> fileNameList)
			throws Exception {

		List<Map<String, String>> layoutMapList = new ArrayList<Map<String, String>>();
		Map<String, String> layoutMap = new HashMap<String, String>();
		String funcName = "parseExportFile";
		
		try {
			for (String fileName : fileNameList) {
				
				String fileNamePath = tableLayoutPath + fileName;
				System.out.println("\n\n=============================");
				System.out.println("fileName:" + fileNamePath);
				
				Workbook wbLayout = Tools.getWorkbook(fileNamePath);
				
				// 防呆 Excel必需要有Layout頁籤
				if (wbLayout.getSheet("Layout") == null)
					throw new Exception(className + " " + funcName + " Error: 缺少頁韱:Layout");
				
				layoutMapList = ParseLayout.run(wbLayout.getSheet("Layout"), mapProp);
				layoutMap = layoutMapList.get(layoutMapList.size()-1);// 取最後一筆Main資料
				String txtFileName = layoutMap.get("TXTFileName");
				if (StringUtils.isBlank(txtFileName))
					throw new Exception(className + " " + funcName + " Error: 缺少文字檔檔名");
	
				List<String> dataStartEndList = new ArrayList<String>();
				List<String> dataColsList = new ArrayList<String>();
				int colLenStart = 0, colLenEnd = 0;
				for (Map<String, String> layoutMapfor : layoutMapList) {
					if ("Detail".equals(layoutMapfor.get("MapType"))) {
						colLenStart = colLenEnd + 1;
						dataStartEndList.add(String.valueOf(colLenStart));
						// 若欄位型態為decimal，則長度寫法會是(?,?)
						// 取第一逗號左邊數值後再加一(因在定長規則裡小數點也會佔一位數)
						// 例: 10,2 -> 長度為11
						String colLen = layoutMapfor.get("ColLen");
						colLen = colLen.contains(",") ? String.valueOf(Integer.parseInt(colLen.substring(0,colLen.indexOf(","))) + 1) : colLen;

						// 因定長原因，故長度要累加
						colLenEnd += Integer.parseInt(colLen);
						
						dataStartEndList.add(String.valueOf(colLenEnd));
						dataColsList.add(layoutMapfor.get("ColEName"));
					}
				}

				fileName = fileName.substring(0, fileName.lastIndexOf("."));
				toExcel(fileName, tableLayoutPath, txtFileName, layoutMapList, dataColsList, dataStartEndList);
			}
		} catch (Exception ex) {
			throw new Exception(className + " " + funcName + " Error: \n" + ex);
		}

		System.out.println(className + " " + funcName + " Done!");
	}

	private static void toExcel(String fileName, String tableLayoutPath, String txtFileName,
			List<Map<String, String>> layoutMapList, List<String> dataColsList,
			List<String> dataStartEndList) throws Exception {

		Row rowSheet1 = null, rowSheet2 = null, rowSheet3 = null;
		Cell cell = null;
		String funcName = "toExcel";
		
		try {

			String outputPath = tableLayoutPath + "../Output/";
			String txtFilePath = tableLayoutPath + "TXTFile/";
			String txtFileContent = FileTools.readFileContent(txtFilePath + txtFileName + ".txt");
			String[] txtFileContentList = txtFileContent.split("\n");
			
			// 整理出Layout頁籤數值型態的欄位
			List<String> listNumTypeColName = new ArrayList<String>();
			List<Map<String, String>> listNumType = new ArrayList<Map<String, String>>();
			List<String> numTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER", "DECIMAL" });
			for (Map<String, String> layoutMapfor : layoutMapList) {
				if ("Detail".equals(layoutMapfor.get("MapType"))) {
					String colType = layoutMapfor.get("ColType").toString().toUpperCase();
					if (numTypeList.contains(colType)) {
						listNumType.add(layoutMapfor);
						listNumTypeColName.add(layoutMapfor.get("ColEName"));
					}
				}
			}
			
	        XSSFWorkbook wbOut = new XSSFWorkbook();
	        XSSFSheet sheet1 = wbOut.createSheet("Full Content");
	        XSSFSheet sheet2 = wbOut.createSheet("Number Type Content");
	        XSSFSheet sheet3 = wbOut.createSheet("Number Type Summary");
	
	        CellStyle style = Tools.setTitleStyle(wbOut);
	        CellStyle styleNum = null;
	        String lastExcelColNameSheet1 = Tools.getLastExcelColName(dataColsList.size());
	        String lastExcelColNameSheet2 = Tools.getLastExcelColName(listNumTypeColName.size());
	
	     	int r = 1, cSheet1 = 0, cSheet3 = 0, line = 0;
	     	Double[] sumNumCol = new Double[dataColsList.size()];
	     	Integer[] decimalPlacesCol = new Integer[dataColsList.size()];
	     	
	        // 設定標題 & 凍結首欄 & 首欄篩選
			Tools.setTitle(sheet1, lastExcelColNameSheet1, style, dataColsList);
			Tools.setTitle(sheet2, lastExcelColNameSheet2, style, listNumTypeColName);
			Tools.setTitle(sheet3, lastExcelColNameSheet2, style, listNumTypeColName);
			
			// 設定內容
			style = Tools.setStyle(wbOut);
			for (String content : txtFileContentList) {
				// 去除表頭表尾
				line ++;
				if(line == 1 || line == txtFileContentList.length)
					continue;
				
				cSheet1 = 0;
				rowSheet1 = sheet1.createRow(r);
				rowSheet2 = sheet2.createRow(r);
				r++;
				for (int i = 0; i < dataStartEndList.size();) {
					int start = Integer.parseInt(dataStartEndList.get(i++)) - 1;
					int end = Integer.parseInt(dataStartEndList.get(i++));
					int dataColsListNum = i / 2 - 1;
					String odsColsName = dataColsList.get(dataColsListNum);
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
									styleNum = Tools.setNumStyle(wbOut,decimalPlaces);
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
			        styleNum = Tools.setNumStyle(wbOut, decimalPlacesCol[i]);
					Tools.setNumericCell(styleNum, cell, rowSheet3, cSheet3++, sumNumCol[i]);
				}
			}
	
			// 將整理好的比對結果另寫出Excel檔
			Tools.output(wbOut, outputPath + fileName + "/", txtFileName + "_" + fileName);
		} catch (Exception ex) {
			throw new Exception(className + " " + funcName + " Error: \n" + ex);
		}

		System.out.println(className + " " + funcName + " Done!");
	}
}
