package gss.SourceFile;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import gss.Tools.Tools;

/**
 * 檢查Not Null欄位是否有空值
 * @author nicole_tsou
 *
 */
public class ChkNotNullContent {
	private static final String className = ChkNotNullContent.class.getName();

	public static void run(String fileNamePath, String fileName, String sourceFileName,
			List<Map<String, String>> layoutMapList) throws Exception {
		
		fileNamePath = fileNamePath+fileName+"/"+sourceFileName+"_"+fileName+".xlsx";
		Workbook workbook = Tools.getWorkbook(fileNamePath);
		Sheet sheet = workbook.getSheet("Full Content");
		Row rowTitle = null;
		boolean isNotNull = false;
		
		try {
			// 解析資料內容
			rowTitle = sheet.getRow(0);
			for (int c = 0; c <= rowTitle.getLastCellNum(); c++) {
				String odsColName = Tools.getCellValue(rowTitle, c, "Title" + c);
				isNotNull = false;
				for (Map<String, String> layoutMap : layoutMapList) {
					if ("Detail".equals(layoutMap.get("MapType"))) {
						if (odsColName.equalsIgnoreCase(layoutMap.get("ColEName"))) {
							isNotNull = "Y".equalsIgnoreCase(layoutMap.get("PK"))
									|| "N".equalsIgnoreCase(layoutMap.get("Nullable")) ? true : false;
						}
					}
				}

				// 若有空值則報錯
				if (isNotNull) {
					for (int r = 1; r <= sheet.getLastRowNum(); r++) {
						String odsColValue = Tools.getCellValue(sheet.getRow(r), c, odsColName);
						if (StringUtils.isBlank(odsColValue))
							throw new Exception(odsColName + "欄位第 " + (r + 1) + " Row不可為空(Row:" + (r + 1) + ",Cell:"
									+ (c + 1) + ")");
					}
				}
			}
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
}
