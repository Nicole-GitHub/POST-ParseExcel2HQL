package gss;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import gss.TableLayout.ParseTableLayout;
import gss.Tools.FileTools;
import gss.Tools.Property;
import gss.Word.ParseControlSheet;
import gss.Word.ParseTestRecordList;
import gss.Word.WriteToPushScript;
import gss.Word.WriteToTestRecordInfo;
import gss.Word.WriteToTestRecordWord;

public class Main {
	private static final String className = Main.class.getName();

	public static void main(String args[]) throws Exception {
		try {
			String os = System.getProperty("os.name");

			System.out.println("=== NOW TIME: " + new Date());
			System.out.println("=== os.name: " + os);
			System.out.println("=== Parser.class.Path: "
					+ Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
			// 判斷當前執行的啟動方式是IDE還是jar
			// 若放檔的路徑中有中文時執行bat會讓中文變亂碼導致使用.isFile()會失效，故用.endsWith判斷
			String runPath = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			boolean isStartupFromJar = runPath.endsWith(".jar");
//		boolean isStartupFromJar = new File(Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath()).isFile();
			System.out.println("=== isStartupFromJar: " + isStartupFromJar);
			String jarPath = System.getProperty("user.dir") + File.separator; // Jar
			if (!isStartupFromJar) {// IDE
				jarPath = os.contains("Mac") ? "/Users/nicole/Dropbox/POST/JavaTools/POST-ParseExcel2HQL/" // Mac
						: "C:\\Users\\nicole_tsou\\Dropbox\\POST\\JavaTools\\POST-ParseExcel2HQL\\"; // win
			}

			// TableLayout Path
			String tableLayoutPath = jarPath + "TableLayout/";
			// 列出TableLayout下的所有檔案(不含隱藏檔)
			List<String> tableLayoutFileNameList = new ArrayList<String>();
			String[] fileName = new File(tableLayoutPath).list();
			for (String str : fileName) {
				File f = new File(tableLayoutPath + str);
				if (!f.isHidden() && f.isFile()) {
					tableLayoutFileNameList.add(str);
//					System.out.println("isNotHiddenFile:" + str);
				}
			}

			System.out.println("Path: " + jarPath 
					+ "\n TableLayoutPath:" + tableLayoutPath 
					+ "\n TableLayoutFileName: " + tableLayoutFileNameList
					);

			String outputPath = jarPath + "Output/";
			FileTools.deleteFolder(outputPath);
			
			
			// Property
			Map<String, String> mapProp = Property.getProperties(jarPath);
//			if("3".equals(mapProp.get("runType"))) {
////				RunParseTXTFile.parseExportFile(mapProp, tableLayoutPath, tableLayoutFileNameList);
//			}else
			
			ParseTableLayout.run(mapProp, tableLayoutPath, outputPath, tableLayoutFileNameList);
			

			String refFilePath = jarPath + "RefFile/";
			
			// 整理測試紀錄各項目的編號最大值
			String testRecordListWordPath = mapProp.get("svnPath") + "DOCUMENT/5-ST/測試紀錄清單.doc";
			Map<String, Integer> testRecordMap = ParseTestRecordList.getTestRecordMaxNum(testRecordListWordPath);
			
			// 欲解析的layout完整檔名整理出純table名
			List<String> fileENameList = new ArrayList<String>();
			for(String tableLayoutFileName : tableLayoutFileNameList)
				fileENameList.add((tableLayoutFileName.split("-")[0]).trim());
			
			// 解析POST第2階段ETL開發管控.xlsx內容
			List<Map<String, String>> controlSheetList = ParseControlSheet.run(refFilePath, fileENameList);

			/**
			 * 匯出程式上版.xlsx 
			 * 並拋出對應的測試紀錄編號
			 */
			Map<String,String> testRecordNumMap = 
					WriteToTestRecordInfo.run(refFilePath, outputPath, testRecordMap, controlSheetList);
			
			// 匯出測試紀錄WORD檔
			WriteToTestRecordWord.run(refFilePath, outputPath, tableLayoutFileNameList, controlSheetList, testRecordNumMap);

			// 匯出上版測試前所需Script
			WriteToPushScript.run(outputPath, controlSheetList, mapProp);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}
	}
	
	
}
