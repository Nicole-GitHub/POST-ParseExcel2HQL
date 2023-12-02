package gss.Word;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.usermodel.Range;

import gss.Tools.Tools;

/**
 * 匯出測試紀錄WORD檔
 * @author nicole_tsou
 *
 */
public class WriteToTestRecordWord {

	/**
	 * @param controlSheetList 解析POST第2階段ETL開發管控.xlsx內容
	 * @param testRecordNumMap 對應的測試紀錄編號
	 * @throws Exception
	 */
	public static void run(String filePath, String outputPath, List<String> tableLayoutFileNameList,
			List<Map<String, String>> controlSheetList, Map<String, String> testRecordNumMap) throws Exception {
		
//		String filePath = "C:\\Users\\nicole_tsou\\Dropbox\\POST\\ETL\\ETL 第二階\\程式\\_TESTING\\";
		String inputPath1 = filePath + "Sample收載-測試紀錄-ETL_PMM_CM_01.doc";
		String inputPath2 = filePath + "Sample梳理-測試紀錄-ETL_PMM_PI_63.doc";

		for(Map<String, String> controlSheetMap : controlSheetList) {

			String targetTableEName = controlSheetMap.get("targetTableEName");
			String targetTableCName = controlSheetMap.get("targetTableCName");
			String testRecordNumStr = testRecordNumMap.get(targetTableEName);
			
			controlSheetMap.put("[Replace_測試規格編號]", testRecordNumStr);
			controlSheetMap.put("[Replace_測試規格名稱]", targetTableEName);
			controlSheetMap.put("[Replace_規格描述]", targetTableCName);
			controlSheetMap.put("[Replace_TargetTable]", targetTableEName);
			controlSheetMap.put("[Replace_ODSTable]", "ODS_"+targetTableEName.substring(2));
			controlSheetMap.put("[Replace_SourceTable]", controlSheetMap.get("sourceTableEName"));
			controlSheetMap.put("[Replace_測試日期]", Tools.getNowForROCYear("yyyy/MM/dd"));
			
			String finalOutputPath = "";
			for(String tableLayoutFileName : tableLayoutFileNameList) {
				if(targetTableEName.equals((tableLayoutFileName.split("-")[0]).trim())) {
					String fileName = tableLayoutFileName.substring(0, tableLayoutFileName.lastIndexOf("."));
					finalOutputPath = outputPath + fileName + "\\測試紀錄-" + testRecordNumStr + ".doc";
				}
			}
			System.out.println(finalOutputPath);
			if(targetTableEName.startsWith("T_"))
				readwriteWord(inputPath1, finalOutputPath, controlSheetMap);
			else
				readwriteWord(inputPath2, finalOutputPath, controlSheetMap);
		}
		
	}

	/**
	 * 实现对word读取和修改操作
	 * 
	 * @param filePath word模板路径和名称
	 * @param map      待填充的数据，从数据库读取
	 */
	private static void readwriteWord(String inputPath, String outputPath, Map<String, String> map)
			throws FileNotFoundException {

		FileInputStream in = null;
		HWPFDocument hdt = null;

		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		FileOutputStream out = null;
		
		try {
			in = new FileInputStream(new File(inputPath));
			hdt = new HWPFDocument(in);
		
			// 读取word文本内容
			Range range = hdt.getRange();
//			System.out.println("range.text():"+range.text());
			
			// 替换文本内容
			for (Map.Entry<String, String> entry : map.entrySet()) {
					range.replaceText(entry.getKey(), entry.getValue());
			}
			
//			System.out.println("outputPath:"+outputPath);
			out = new FileOutputStream(outputPath, true);
			hdt.write(ostream);
			// 输出字节流
			out.write(ostream.toByteArray());
			out.close();
			ostream.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

}
