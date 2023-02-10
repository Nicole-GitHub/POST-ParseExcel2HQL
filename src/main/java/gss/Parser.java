package gss;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import gss.tableLayout.RunParseTableLayout;
import gss.txt.RunParseTXT;

public class Parser {
	private static final String className = Parser.class.getName();

	public static void main(String args[]) throws Exception {
		try {
			String os = System.getProperty("os.name");

			System.out.println("=== NOW TIME: " + new Date());
			System.out.println("=== os.name: " + os);
			System.out.println("=== Parser.class.Path: "
					+ Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath());
			// 判斷當前執行的啟動方式是IDE還是jar
			// 若放檔的路徑中有中文時執行bat會讓中文變亂碼導致使用.isFile()會失效，故用.endsWith判斷
			String runPath = Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath();
			boolean isStartupFromJar = runPath.endsWith(".jar");
//		boolean isStartupFromJar = new File(Parser.class.getProtectionDomain().getCodeSource().getLocation().getPath()).isFile();
			System.out.println("=== isStartupFromJar: " + isStartupFromJar);
//		String fileName = "";
			String path = System.getProperty("user.dir") + File.separator; // Jar
//		System.out.println("=== Parser.class.Path2: "+path);
			if (!isStartupFromJar) {// IDE
				path = os.contains("Mac") ? "/Users/nicole/Dropbox/POST/POST-ParseExcel2HQL/" // Mac
						: "C:/Users/nicole_tsou/Dropbox/POST/POST-ParseExcel2HQL/"; // win
			}

			// TableLayout Path
			String tableLayoutPath = path + "TableLayout/";
			// 列出TableLayout下的所有檔案(不含隱藏檔)
			List<String> tableLayoutFileNameList = new ArrayList<String>();
			String[] fileName = new File(tableLayoutPath).list();
			for (String str : fileName) {
				if (new File(path + str).isHidden()) {
					System.out.println("isHidden:" + str);
				} else {
					tableLayoutFileNameList.add(str);
					System.out.println("isNotHidden:" + str);
				}
			}

//			// 來源文字檔 Path
//			String txtPath = path + "TXT/";
//			// 列出TableLayout下的所有檔案(不含隱藏檔)
//			List<String> txtFileNameList = new ArrayList<String>();
//			fileName = new File(txtPath).list();
//			for (String str : fileName) {
//				if (new File(path + str).isHidden()) {
//					System.out.println("isHidden:" + str);
//				} else {
//					txtFileNameList.add(str);
//					System.out.println("isNotHidden:" + str);
//				}
//			}

			System.out.println("Path: " + path 
					+ "\n TableLayoutPath:" + tableLayoutPath 
//					+ "\n TXTPath:" + txtPath
					+ "\n TableLayoutFileName: " + tableLayoutFileNameList
//					+ "\n txtFileName: " + txtFileNameList
					);

			RunParseTableLayout.run(tableLayoutPath, tableLayoutFileNameList);
//			RunParseTXT.run(txtPath, txtFileNameList);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}
	}
	
	
}
