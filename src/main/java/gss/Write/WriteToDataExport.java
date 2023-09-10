package gss.Write;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import gss.ETLCode.bin.EXPORTFILE;
import gss.Tools.FileTools;

public class WriteToDataExport {
	private static final String className = WriteToDataExport.class.getName();

	/**
	 * 資料提供HQL
	 * 
	 * @param outputPath
	 * @param fileName
	 * @param layoutMapList
	 * @param mapProp
	 * @throws Exception
	 */
	public static void run(String outputPath, String fileName, List<Map<String, String>> layoutMapList,
			Map<String, String> mapProp) throws Exception {

		try {
	     	List<String> charTypeList = Arrays.asList(new String[] { "VARCHAR", "CHAR" });
			List<String> intTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER" });
	        
	     	// list的最後一筆位置
			String tableName = layoutMapList.get(layoutMapList.size() - 1).get("TableName");
			String colENameStr = "", colLogic = "", colLogicStr = "", dateFormat = "yyyyMMdd",
					dtFormat = "yyyyMMddHHmmss";
			for (Map<String, String> layoutMap : layoutMapList) {
				if ("Detail".equals(layoutMap.get("MapType"))) {
					String colEName = layoutMap.get("ColEName").toString().toUpperCase();
					String colType = layoutMap.get("ColType").toString().toUpperCase();
					String colLen = layoutMap.get("ColLen").toString().toUpperCase();
					colENameStr += "\t"+colEName + ",\n";
					
					if (charTypeList.contains(colType))
						colLogic = "\trpad(nvl(" + colEName + ",'')," + colLen + ",' ') as " + colEName + ",\n";
					else if (intTypeList.contains(colType))
						colLogic = "\tlpad(nvl(" + colEName + ",'')," + colLen + ",' ') as " + colEName + ",\n";
					else if ("DATE".equals(colType))
						colLogic = "\tlpad(nvl(date_format(" + colEName + ", '" + dateFormat + "'), ''), " + colLen
								+ ", ' ') as " + colEName + ",\n";
					else if ("DATETIME".equals(colType))
						colLogic = "\tlpad(nvl(date_format(" + colEName + ", '" + dtFormat + "'), ''), " + colLen
								+ ", ' ') as " + colEName + ",\n";
					else if ("DECIMAL".equals(colType)) {
						String[] colLenArr = colLen.split(",");
						int colLenInt = Integer.parseInt(colLenArr[0]) + 1;
						
						String format = "0.";
						for(int i = 0 ; i < Integer.parseInt(colLenArr[1]) ; i++)
							format += "0";
						
						colLogic = "\tlpad(nvl(format_number(" + colEName + ", \"" + format + "\"),''), " + colLenInt
								+ ", ' ') as " + colEName + ",\n";
					}
					colLogicStr += colLogic;
				}
			}

			colLogicStr = StringUtils.isBlank(colLogicStr) ? "" : colLogicStr.substring(0,colLogicStr.length() - 2);
			colENameStr = StringUtils.isBlank(colENameStr) ? "" : colENameStr.substring(0,colENameStr.length() - 2);
			String hql = EXPORTFILE.getHQL(mapProp, colLogicStr, colENameStr, tableName);
			String sh = EXPORTFILE.getShell(mapProp, tableName);
			String var = EXPORTFILE.getShellVAR(mapProp, tableName);

			FileTools.createFile(outputPath + fileName + "/bin/", "EXPORTFILE", "hql", hql);
			FileTools.createFile(outputPath + fileName + "/bin/", "EXPORTFILE", "sh", sh);
			FileTools.createFile(outputPath + fileName + "/bin/", "EXPORTFILE", "var", var);
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}

}
