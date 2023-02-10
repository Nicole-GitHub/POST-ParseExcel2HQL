package gss.txt;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import gss.tools.FileTools;
import gss.tools.Tools;

public class RunParseTXT {
	private static final String className = RunParseTXT.class.getName();

	public static void run(String path, String fileName, String dataStartEnd) throws Exception {

		Row row;
		Cell cell;

		try {
			System.out.println("fileName:" + path + fileName);
			String txtContent = FileTools.readFileContent(path + fileName);

			System.out.println("txtContent:" + txtContent);
			String[] txtList = txtContent.split("\n");
			String[] dataStartEndList = dataStartEnd.split(",");

//			/**
//			 * 開始比對Table Layout內容，並將Teradata SQL 內的Schema另寫成新的Excel檔，並標註比對結果(紅底表示不一致)
//			 */
//			// 因output時需workbook所以多此行只為取workbook
//			Workbook targetTableWorkbook = Tools.getWorkbook(targetTableLayoutExcelPath+"../Sample - Table Layout.xlsx");
//			Sheet targetSheet = targetTableWorkbook.getSheet("Layout");
//			CellStyle cellStyleNormal = Tools.setStyle(targetTableWorkbook);
//			CellStyle cellStyleError = Tools.setStyleError(targetTableWorkbook);
//			
//			Tools.setCell(tableName.equals(getCellValue(sourceSheet.getRow(0), 4, "TABLE名稱")) ? cellStyleNormal
//					: cellStyleError, targetSheet.getRow(0), 4, tableName);
//			
//			boolean excelEqualsSql = false;
//			int lastRowNum = 0;
//			for (int i = 4; i <= sourceSheet.getLastRowNum(); i++) {
//				row = sourceSheet.getRow(i);
//				cell = row == null ? null : row.getCell(0);
//				
//				if (cell == null || StringUtils.isBlank(cell.toString())) {
//					break;
//				} else {
//					String excelColName = getCellValue(row,1,"欄位名稱").toUpperCase();
//					String excelColType = getCellValue(row,3,"資料型態").toUpperCase();
//					String excelColLen = getCellValue(row,4,"資料長度").toUpperCase().replace("(", "").replace(")", "").replace(" ", "");
//					String excelColNull = getCellValue(row,6,"NULL註記").toUpperCase();
//					String excelColPK = getCellValue(row,5,"主鍵註記").toUpperCase();
//					
//					row = targetSheet.createRow(i);
//					excelEqualsSql = false;
//					for(Map<String, String> mapLayout : mapListSQLLayout) {
//						String sqlColName = mapLayout.get("ColName").toUpperCase();
//						if(excelColName.equals(sqlColName)) {
//							excelEqualsSql = true;
//							String sqlColType = mapLayout.get("ColType").toUpperCase();
//							String sqlColLen = mapLayout.get("ColLen").toUpperCase();
//							String sqlColNull = mapLayout.get("ColNull").toUpperCase();
//							String sqlColPK = mapLayout.get("ColPK").toUpperCase();
//
//							CellStyle sqlColTypeCellStyle = excelColType.equals(sqlColType) ? cellStyleNormal : cellStyleError;
//							CellStyle sqlColLenCellStyle = excelColLen.equals(sqlColLen) ? cellStyleNormal : cellStyleError;
//							CellStyle sqlColNullCellStyle = excelColNull.equals(sqlColNull) ? cellStyleNormal : cellStyleError;
//							CellStyle sqlColPKCellStyle = excelColPK.equals(sqlColPK) ? cellStyleNormal	: cellStyleError;
//							
//							isError = (sqlColTypeCellStyle.equals(cellStyleError)
//									|| sqlColLenCellStyle.equals(cellStyleError)
//									|| sqlColNullCellStyle.equals(cellStyleError)
//									|| sqlColPKCellStyle.equals(cellStyleError)) ? true : false;
//							
//							cell = row.createCell(0);
//							cell.setCellFormula("ROW()-4");
//							cell.setCellStyle(cellStyleNormal);
//							Tools.setCell(cellStyleNormal, row, 1, sqlColName);
//							Tools.setCell(sqlColTypeCellStyle,  row, 3, sqlColType);
//							Tools.setCell(sqlColLenCellStyle,  row, 4, sqlColLen);
//							Tools.setCell(sqlColNullCellStyle,  row, 6, sqlColNull);
//							Tools.setCell(sqlColPKCellStyle,  row, 5, sqlColPK);
//							break;
//						}
//					}
//					// 若Excel內的欄位名稱比對不到SQL的欄位則另執行此段
//					if(!excelEqualsSql) {
//						cell = row.createCell(0);
//						cell.setCellFormula("ROW()-4");
//						cell.setCellStyle(cellStyleError);
//						Tools.setCell(cellStyleError, row, 1, excelColName);
//						Tools.setCell(cellStyleError, row, 2, "(Script無此欄位)");
//						Tools.setCell(cellStyleError, row, 3, "");
//						Tools.setCell(cellStyleError, row, 4, "");
//						Tools.setCell(cellStyleError, row, 5, "");
//						Tools.setCell(cellStyleError, row, 6, "");
//						isError = true;
//					}
//					lastRowNum = i;
//				}
//			}

			// 將整理好的比對結果另寫出Excel檔
//			Tools.output(targetTableWorkbook, "2007", targetTableLayoutExcelPath, "Target - " + subSys + " " + fileName);
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
}
