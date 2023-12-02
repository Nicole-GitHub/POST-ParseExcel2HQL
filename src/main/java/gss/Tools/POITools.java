package gss.Tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class POITools {
	private static final String className = POITools.class.getName();

	public static void main(String arg[]) {
//		System.out.println(getNowForROCYear("yyyy/MM/dd"));
	}
	
	/**
	 * 取得 Excel的Workbook
	 * 
	 * @param path
	 * @return
	 */
	public static Workbook getWorkbook(String path) {
		Workbook workbook = null;
		InputStream inputStream = null;
		try {
			File f = new File(path);
			inputStream = new FileInputStream(f);
			String aux = path.substring(path.lastIndexOf(".") + 1);
			if ("XLS".equalsIgnoreCase(aux)) {
				workbook = new HSSFWorkbook(inputStream);
			} else if ("XLSX".equalsIgnoreCase(aux)) {
				workbook = new XSSFWorkbook(inputStream);
			} else {
				throw new Exception("檔案格式錯誤");
			}

		} catch (Exception ex) {
			// 因output時需要用到，故不可寫在finally內
			try {
				if (workbook != null)
					workbook.close();
			} catch (IOException e) {
				throw new RuntimeException(className + " getWorkbook Error: \n" + e);
			}

			throw new RuntimeException(className + " getWorkbook Error: \n" + ex);
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
			} catch (IOException e) {
				throw new RuntimeException(className + " getWorkbook Error: \n" + e);
			}
		}
		return workbook;
	}

	/**
	 * 寫出整理好的Excel檔案
	 * 
	 * @param outputPath
	 * @param outputFileName
	 */
	public static void output(Workbook workbook, String outputPath, String outputFileName) {
		OutputStream output = null;
		File f = null;
		
		try {
			f = new File(outputPath);
			if(!f.exists()) f.mkdirs();
			
			f = new File(outputPath + outputFileName + ".xlsx");
			output = new FileOutputStream(f);
			workbook.write(output);
		} catch (Exception ex) {
			throw new RuntimeException (className + " output Error: \n" + ex);
		} finally {
			try {
				if (workbook != null)
					workbook.close();
				if (output != null)
					output.close();
			} catch (IOException ex) {
				throw new RuntimeException (className + " output finally Error: \n" + ex);
			}
		}
	}

	/**
	 * 設定寫出檔案時的Style
	 */
	public static CellStyle setNumStyle(Workbook workbook, int num) {
		// 將小數位數改為format格式
		String formatStr = "0";
		for(int i = 0 ; i < num ; i++)
			formatStr += ((i == 0) ? "." : "") + "0";
		
		CellStyle style = setStyle(workbook);
		DataFormat format = workbook.createDataFormat(); 
		style.setDataFormat(format.getFormat(formatStr));
		style.setFont(setFont(workbook));
		return style;
	}
	
	/**
	 * 設定寫出檔案時的Style
	 */
	public static CellStyle setStyle(Workbook workbook) {
		CellStyle style = workbook.createCellStyle();
		short BorderStyle = CellStyle.BORDER_THIN;
		style.setBorderBottom(BorderStyle); // 儲存格格線(下)
		style.setBorderLeft(BorderStyle); // 儲存格格線(左)
		style.setBorderRight(BorderStyle); // 儲存格格線(右)
		style.setBorderTop(BorderStyle); // 儲存格格線(上)
		style.setFont(setFont(workbook));
		return style;
	}
	
	/**
	 * 設定寫出檔案時的Style(標題)
	 * @param workbook
	 * @return
	 */
	public static CellStyle setTitleStyle(Workbook workbook) {
		CellStyle style = workbook.createCellStyle();
		style.setFillForegroundColor(HSSFColor.LIGHT_GREEN.index);// 儲存格底色為:紅色
		style.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);
		short BorderStyle = CellStyle.BORDER_THIN;
		style.setBorderBottom(BorderStyle); // 儲存格格線(下)
		style.setBorderLeft(BorderStyle); // 儲存格格線(左)
		style.setBorderRight(BorderStyle); // 儲存格格線(右)
		style.setBorderTop(BorderStyle); // 儲存格格線(上)
		style.setFont(setFont(workbook));
		return style;
	}

	/**
	 * 設定字型 14pt 微軟正黑體
	 * @param workbook
	 * @return
	 */
	private static Font setFont(Workbook workbook) {
		Font font = workbook.createFont();
		font.setFontHeightInPoints((short) 12);
		font.setFontName("微軟正黑體");
		return font;
	}
		
	/**
	 * 設定字串Cell內容(含Style)
	 * @param style
	 * @param cell
	 * @param row
	 * @param cellNum
	 * @param cellValue
	 */
	public static void setStringCell(CellStyle style, Cell cell, Row row, int cellNum, String cellValue) {
		cell = row.createCell(cellNum,Cell.CELL_TYPE_STRING);
		cell.setCellValue(cellValue);
		cell.setCellStyle(style);
	}
	

	/**
	 * 設定數值Cell內容(含Style)
	 * @param style
	 * @param cell
	 * @param row
	 * @param cellNum
	 * @param cellValue
	 */
	public static void setNumericCell(CellStyle style, Cell cell, Row row, int cellNum, double cellValue) {
		cell = row.createCell(cellNum,Cell.CELL_TYPE_NUMERIC);
		cell.setCellValue(cellValue);
		cell.setCellStyle(style);
	}
	
	
	
	/**
	 * 取Excel欄位值
	 * 
	 * @param sheet
	 * @param rownum
	 * @param cellnum
	 * @param fieldName
	 * @return
	 * @throws Exception 
	 */
	public static String getCellValue(Row row, int cellnum, String fieldName) throws Exception {
		try {
			Cell cell = row.getCell(cellnum);
			if (!cellNotBlank(cell))
				return "";
			else if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC)
				return String.valueOf((int) cell.getNumericCellValue()).trim();
			else if (cell.getCellType() == Cell.CELL_TYPE_STRING)
				return cell.getStringCellValue().trim();
			else if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
				if (cell.getCachedFormulaResultType() == Cell.CELL_TYPE_NUMERIC)
					return String.valueOf((int) cell.getNumericCellValue()).trim();
				else if (cell.getCachedFormulaResultType() == Cell.CELL_TYPE_STRING)
					return cell.getStringCellValue().trim();
			}
		} catch (Exception ex) {
			throw new Exception(className + " getCellValue " + fieldName + " 格式錯誤");
		}
		return "";
	}
	
	/**
     * 不為空
     */
	public static boolean cellNotBlank(Cell cell) {
		return cell != null && cell.getCellType() != Cell.CELL_TYPE_BLANK;
	}
	

	/**
	 * 取最後一欄的Excel名稱(例:BH欄)
	 * @param len
	 * @return
	 */
	public static String getLastExcelColName(int len) {
		List<String> alphabet = Arrays.asList(new String[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
				"M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" });
		int col1 = len / 26;
		int col2 = len % 26;
		
		String rs = col1 > 0 ? alphabet.get(col1 - 1) : "";
		rs += col2 > 0 ? alphabet.get(col2 - 1) : "";
		
		return rs;
	}
	
	/**
	 * 設定標題 & 凍結首欄 & 首欄篩選
	 * 
	 * @param sheet
	 * @param lastExcelColNameSheet
	 * @param style
	 * @param listColName
	 * @throws Exception
	 */
	public static void setTitle(Sheet sheet, String lastExcelColNameSheet, CellStyle style, List<String> listColName)
			throws Exception {
		Cell cell = null;
		int c = 0;
        sheet.createFreezePane(0,1,0,1); // 凍結首行
		sheet.setAutoFilter(CellRangeAddress.valueOf("A1:" + lastExcelColNameSheet + "1")); // 設定篩選欄A1:ZZ1
		Row rowSheet = sheet.createRow(0);
		for (String col : listColName) {
			setStringCell(style, cell, rowSheet, c++, col);
		}
	}

	/**
	 * 判斷是否有刪除線 (僅限xlsx使用)
	 * 
	 * @param row
	 * @param cellNum
	 * @return
	 */
	public static Boolean isDelLine(Row row, int cellNum) {
		if(!cellNotBlank(row.getCell(cellNum))) {
			return false;
			
		// 2003版的語法不work(好像是)
//		}else if ("2003".equals(excelVersion)) {
//			return ((HSSFCellStyle) row.getCell(cellNum).getCellStyle()).getFont(workbook).getStrikeout();
		} else {
			return ((XSSFCellStyle) row.getCell(cellNum).getCellStyle()).getFont().getStrikeout();
		}
	}
}
