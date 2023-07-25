package gss.ETLCode;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class RCPT {
	
	/**
	 * RCPT 內容
	 * @param raw
	 * @param rcptODSColLogic
	 * @param rcptColLogic
	 * @param odsTableName
	 * @param tableName
	 * @return
	 */
	public static String getHQL(Map<String, String> mapProp, String rcptODSColLogic, String rcptColLogic, String odsTableName,
			String tableName) {
		
		String raw = mapProp.get("hadoop.raw.dbname");
		String type = mapProp.get("runType");
		
		if("1".equals(type)) {
			String dwRs = "-- 總筆數\n" 
					+ "select count(1) cnt from " + raw + "." + odsTableName + " ;\n"
					+ "select count(1) cnt from " + raw + "." + tableName + " ;\n\n" ;
			String verifySum = 
					 "-- 數值加總\n"
					+ "Select \n" + rcptODSColLogic + "\n"
					+ "FROM " + raw + "." + odsTableName + " T1 ;\n\n" 
					+ "Select \n" + rcptColLogic + "\n"
					+ "FROM " + raw + "." + tableName + " T1 ;\n" + "\n" ;
			dwRs += StringUtils.isBlank(rcptODSColLogic) ? "-- 無數值欄位，故不需驗證數值欄位的加總--\n\n" : verifySum;
			dwRs += "-- 維度分群加總\n"
					+ "select [維度欄位], count(1) cnt FROM " + raw + "." + odsTableName + " T1 GROUP BY [維度欄位];\n" 
					+ "select [維度欄位], count(1) cnt FROM " + raw + "." + tableName + " T1 GROUP BY [維度欄位];\n\n" 
					+ "-- 資料抽樣\n"
					+ "SELECT *\n" 
					+ "FROM " + raw + "." + odsTableName + " T1\n"
					+ "WHERE [PK欄位] = '' ;\n\n" 
					+ "SELECT *\n" + "FROM " + raw + "." + tableName + " T1\n" 
					+ "WHERE [PK欄位] = '' ;\n";

			return dwRs;
		} else {
			String dmRs = "-- 總筆數\n" 
					+ "select count(1) cnt from " + raw + ".[DW_SourceTable] ;\n"
					+ "select count(1) cnt from " + raw + "." + tableName + " ;\n\n" ;
			String verifySum = 
					 "-- 數值加總\n"
					+ "Select \n" + rcptColLogic + "\n"
					+ "FROM " + raw + ".[DW_SourceTable] T1 ;\n\n" 
					+ "Select \n" + rcptColLogic + "\n"
					+ "FROM " + raw + "." + tableName + " T1 ;\n" + "\n" ;
			dmRs += StringUtils.isBlank(rcptColLogic) ? "-- 無數值欄位，故不需驗證數值欄位的加總--\n\n" : verifySum;
			dmRs += "-- 維度分群加總\n"
					+ "select [維度欄位], count(1) cnt FROM " + raw + ".[DW_SourceTable] T1 GROUP BY [維度欄位];\n" 
					+ "select [維度欄位], count(1) cnt FROM " + raw + "." + tableName + " T1 GROUP BY [維度欄位];\n\n" 
					+ "-- 資料抽樣\n"
					+ "SELECT *\n" 
					+ "FROM " + raw + ".[DW_SourceTable] T1\n"
					+ "WHERE [PK欄位] = '' ;\n\n" 
					+ "SELECT *\n" + "FROM " + raw + "." + tableName + " T1\n" 
					+ "WHERE [PK欄位] = '' ;\n";

			return dmRs;
		}
	}

}
