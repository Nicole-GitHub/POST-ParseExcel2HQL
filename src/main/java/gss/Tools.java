package gss;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Tools {
	private static final String className = Tools.class.getName();

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
	 * 取得 Excel的Sheet
	 * 
	 * @param path
	 * @return
	 */
	public static Sheet getSheet(String path,String sheetName) {
		return getWorkbook(path).getSheet(sheetName);
	}

	/**
	 * 寫出整理好的Excel檔案
	 * 
	 * @param outputPath
	 * @param outputFileName
	 */
	public static void output(Workbook workbook, String excelVersion, String outputPath, String outputFileName) {
		OutputStream output = null;
		File f = null;
		
		try {
			String lastFileName =  outputPath.substring(0,outputPath.length()-1);
			lastFileName = lastFileName.substring(lastFileName.lastIndexOf("/")+1);
			
			f = new File(outputPath + (lastFileName.contains("MD") ? "/CSV" : ""));
			if(!f.exists()) f.mkdirs();
			
			f = new File(outputPath + outputFileName + ("2003".equals(excelVersion) ? ".xls" : ".xlsx"));
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
	 * 判斷是否有刪除線
	 * 
	 * @param row
	 * @param cellNum
	 * @return
	 */
	protected static Boolean isDelLine(Workbook workbook, String excelVersion, Row row, int cellNum) {
		if(!isntBlank(row.getCell(cellNum))) {
			return false;
		}else if ("2003".equals(excelVersion)) {
			return ((HSSFCellStyle) row.getCell(cellNum).getCellStyle()).getFont(workbook).getStrikeout();
		} else {
			return ((XSSFCellStyle) row.getCell(cellNum).getCellStyle()).getFont().getStrikeout();
		}
	}

	/**
	 * 設定寫出檔案時的Style
	 */
	protected static CellStyle setStyle(Workbook workbook) {
		CellStyle style = workbook.createCellStyle();
		short BorderStyle = CellStyle.BORDER_THIN;
		style.setBorderBottom(BorderStyle); // 儲存格格線(下)
		style.setBorderLeft(BorderStyle); // 儲存格格線(左)
		style.setBorderRight(BorderStyle); // 儲存格格線(右)
		style.setBorderTop(BorderStyle); // 儲存格格線(上)
		return style;
	}

	/**
	 * 設定Cell內容(含Style)
	 * 
	 * @param cell
	 * @param row
	 * @param cellNum
	 * @param cellValue
	 */
	public static void setCell(CellStyle style, Cell cell, Row row, int cellNum, String cellValue) {
		cell = row.createCell(cellNum);
		cell.setCellValue(cellValue);
		cell.setCellStyle(style);
	}
	
	/**
     * 不為空
     */
	protected static boolean isntBlank(Cell cell) {
		return cell != null && cell.getCellType() != Cell.CELL_TYPE_BLANK;
	}
	
	/**
	 * 中文欄位
	 *  部份、身份 -> 部分、身分
	 *  計劃 -> 計畫
	 * 	迄 -> 訖
	 *  記錄 -> 紀錄
	 */
	protected static String replaceFieldCName(String fieldCName) {

		fieldCName = fieldCName.replace("部份", "部分");
		fieldCName = fieldCName.replace("身份", "身分");
		fieldCName = fieldCName.replace("計劃", "計畫");
		fieldCName = fieldCName.replace("迄", "訖");
		fieldCName = fieldCName.replace("記錄", "紀錄");
		return fieldCName;
	}
	
	/**
	 * 是否為需加密的欄位
	 * sourceFieldEName.equals("HOSP_ID") || fieldCName.equals("ID")
	 *	|| fieldCName.contains("身分證字號") || fieldCName.contains("身分證號") 
	 *	|| fieldCName.contains("醫事機構代碼") || fieldCName.contains("特約藥局代號") 
	 *	|| fieldCName.contains("保險對象健保ID") || fieldCName.contains("醫師ID") 
	 *	|| fieldCName.contains("藥師ID")
	 *	|| fieldCName.contains("投保單位或扣費單位") || fieldCName.contains("統一編號")
	 */
	public static boolean isEncrypt(String fieldCName, String sourceFieldEName) {
		List<String> list = Arrays.asList(
				new String[] {"身分證字號","醫事機構代碼","保險對象健保ID","藥師ID","投保單位或扣費單位","身分證號","特約藥局代號","醫師ID","統一編號","醫事人員代號"});
		
		if(sourceFieldEName.equals("HOSP_ID") || fieldCName.equals("ID"))
			return true;
		
		for(String str : list) {
			if(fieldCName.contains(str))
				return true;
		}
		
		return false;
	}
	

	/**
	 * 寫入 REQandTableLayout Cell
	 */
	public static void setREQandTLCell(CellStyle style, Cell cell, Row row, String dataLineSeq, String dataStartDate, String fieldEName, String fieldCName, String dataType, 
			String dataLength, String pk, String nullable, String initValue, String isEncrypt, String type, String dataClass, 
			String dataDesc, String sourceTableEName, String sourceFieldEName, String procRule, String chgDesc) {
		
		setCell(style, cell, row, 0, dataLineSeq);
		setCell(style, cell, row, 1, fieldEName);
		setCell(style, cell, row, 2, fieldCName);
		setCell(style, cell, row, 3, dataType);
		setCell(style, cell, row, 4, dataLength);
		setCell(style, cell, row, 5, pk);
		setCell(style, cell, row, 6, nullable);
		setCell(style, cell, row, 7, initValue);
		setCell(style, cell, row, 8, isEncrypt);
		if ("REQ".equals(type)) {
			setCell(style, cell, row, 9, dataClass);
			setCell(style, cell, row, 10, dataDesc);
			setCell(style, cell, row, 11, sourceTableEName);
			setCell(style, cell, row, 12, sourceFieldEName);
			setCell(style, cell, row, 13, procRule);
			setCell(style, cell, row, 14, chgDesc);
		} else {
			setCell(style, cell, row, 9, dataLineSeq);
			setCell(style, cell, row, 10, "");
			setCell(style, cell, row, 11, dataStartDate);
			setCell(style, cell, row, 12, "");
			setCell(style, cell, row, 13, dataClass);
			setCell(style, cell, row, 14, dataDesc);
			setCell(style, cell, row, 15, "");
			setCell(style, cell, row, 16, "");
		}
	}
	
	/**
	 * 寫入 DataMart Cell
	 */
	public static void setDataMartCell(CellStyle style, Cell cell, Row row, String dwTableEName, String fieldEName, String dataSource, String sourceTableEName, 
			String tableCName, String sourceFieldEName, String fieldCName, String procRule) {

		setCell(style, cell, row, 0, dwTableEName);
		setCell(style, cell, row, 1, fieldEName);
		setCell(style, cell, row, 2, dataSource);
		setCell(style, cell, row, 3, sourceTableEName);
		setCell(style, cell, row, 4, tableCName);
		setCell(style, cell, row, 5, sourceFieldEName);
		setCell(style, cell, row, 6, fieldCName);
		setCell(style, cell, row, 7, procRule);
		setCell(style, cell, row, 8, "DW");
		
	}

	/**
	 * 取今日日期 YYYY/MM/DD
	 * 
	 * @return
	 */
	public static String getToDay() {
		return new SimpleDateFormat("yyyy/MM/dd").format(new Date());
	}
}
