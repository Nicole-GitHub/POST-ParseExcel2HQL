package gss;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import gss.TableLayout.RunParseTableLayout;
import gss.Tools.FileTools;
import gss.Tools.Property;

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
			String path = System.getProperty("user.dir") + File.separator; // Jar
			if (!isStartupFromJar) {// IDE
				path = os.contains("Mac") ? "/Users/nicole/Dropbox/POST/JavaTools/POST-ParseExcel2HQL/" // Mac
						: "C:/Users/nicole_tsou/Dropbox/POST/JavaTools/POST-ParseExcel2HQL/"; // win
//						: "C:/Users/Nicole/Dropbox/POST/POST-ParseExcel2HQL/"; // win(MSI)
			}

			// TableLayout Path
			String tableLayoutPath = path + "TableLayout/";
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

			System.out.println("Path: " + path 
					+ "\n TableLayoutPath:" + tableLayoutPath 
					+ "\n TableLayoutFileName: " + tableLayoutFileNameList
					);

			FileTools.deleteFolder(path + "Output/");

			// Property
			Map<String, String> mapProp = Property.getProperties(path);
			if("3".equals(mapProp.get("runType"))) {
//				RunParseTXTFile.parseExportFile(mapProp, tableLayoutPath, tableLayoutFileNameList);
			}else
				RunParseTableLayout.run(mapProp, tableLayoutPath, tableLayoutFileNameList);
			
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}
	}
	
	
}
