package gss.Write;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import gss.Tools.FileTools;

/**
 * 產出測試時所需SQL
 * @author nicole_tsou
 *
 */
public class WriteToRCPT {
	private static final String className = WriteToRCPT.class.getName();
	
	public static void run(String outputPath, String fileName, List<Map<String, String>> layoutMapList, Map<String, String> mapProp)
			throws Exception {
		try {
			List<String> charTypeList = Arrays.asList(new String[] { "VARCHAR", "CHAR" });
			List<String> intTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER" });
	        
			// list的最後一筆位置
			int layoutMapListLastNum = layoutMapList.size() - 1;
			String tableName = layoutMapList.get(layoutMapListLastNum).get("TableName");
	     	String odsTableName = "ODS" + tableName.substring(1);
			String rcptODSColLogic = "", colLogic = "", rcptColLogic = "";
			for (Map<String, String> layoutMap : layoutMapList) {
				if ("Detail".equals(layoutMap.get("MapType"))) {
					String colEName = layoutMap.get("ColEName").toString().toUpperCase();
					String colType = layoutMap.get("ColType").toString().toUpperCase();
					String colLen = layoutMap.get("ColLen").toString().toUpperCase();

					if (intTypeList.contains(colType) || "DECIMAL".equals(colType)) {
						colLogic = WriteToLogic.getColLogic(charTypeList, intTypeList, colEName, colType, colLen);
						if (intTypeList.contains(colType)) {
							rcptODSColLogic += "\tsum(" + colLogic + ") as " + colEName + " ,\n";
							rcptColLogic += "\tsum(" + colEName + ") as " + colEName + " ,\n";
						} else if ("DECIMAL".equals(colType)) {
							rcptODSColLogic += "\tsum(" + colLogic + ") as " + colEName + " ,\n";
							rcptColLogic += "\tsum(" + colEName + ") as " + colEName + " ,\n";
						}
					}
				}
			}
			
			String raw = mapProp.get("hadoop.raw.dbname");
			rcptODSColLogic = StringUtils.isBlank(rcptODSColLogic) ? "" : rcptODSColLogic.substring(0,rcptODSColLogic.length() - 2);
			rcptColLogic = StringUtils.isBlank(rcptColLogic) ? "" : rcptColLogic.substring(0,rcptColLogic.length() - 2);
			String sql = "-- 總筆數\n" 
					+ "select count(1) cnt from " + raw + "." + odsTableName + " ;\n"
					+ "select count(1) cnt from " + raw + "." + tableName + " ;\n\n" 
					+ "-- 數值加總\n"
					+ "Select \n" + rcptODSColLogic + "\n"
					+ "FROM " + raw + "." + odsTableName + " T1 ;\n\n" 
					+ "Select \n" + rcptColLogic + "\n"
					+ "FROM " + raw + "." + tableName + " T1 ;\n" + "\n" 
					+ "-- 維度分群加總\n"
					+ "select POLICY_KIND,count(1) cnt FROM " + raw + "." + odsTableName + " T1 GROUP BY POLICY_KIND;\n" 
					+ "select POLICY_KIND,count(1) cnt FROM " + raw + "." + tableName + " T1 GROUP BY POLICY_KIND;\n\n" 
					+ "-- 資料抽樣\n"
					+ "SELECT *\n" 
					+ "FROM " + raw + "." + odsTableName + " T1\n"
					+ "WHERE POLICY_NO = '' ;\n\n" 
					+ "SELECT *\n" + "FROM " + raw + "." + tableName + " T1\n" 
					+ "WHERE POLICY_NO = '' ;";

			FileTools.createFile(outputPath + fileName + "/", "RCPT", "sql", sql);
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
}
