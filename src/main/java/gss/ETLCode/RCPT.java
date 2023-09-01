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
		String sourceTable = "1".equals(type) ? odsTableName : "[DW_SourceTable]";
		String rs = "-- Result\n" 
				+ "select * from post1_post_poc_tmp."+tableName+"_result order by create_time desc;\n"
				+ "select count(1) cnt from post1_post_poc_tmp."+tableName+" ;\n\n";
		
//		if("1".equals(type)) {
			rs += "-- 總筆數\n" 
					+ "select count(1) cnt from " + raw + "." + sourceTable + " ;\n"
					+ "select count(1) cnt from " + raw + "." + tableName + " ;\n\n" ;
			String verifySum = 
					 "-- 數值加總\n"
					+ "Select \n" + ("1".equals(type) ? rcptODSColLogic : rcptColLogic) + "\n"
					+ "FROM " + raw + "." + sourceTable + " T1 ;\n\n" 
					+ "Select \n" + rcptColLogic + "\n"
					+ "FROM " + raw + "." + tableName + " T1 ;\n" + "\n" ;
			rs += StringUtils.isBlank(rcptODSColLogic) ? "-- 無數值欄位，故不需驗證數值欄位的加總--\n\n" : verifySum;
			rs += "-- 維度分群加總\n"
					+ "select [維度欄位], count(1) cnt FROM " + raw + "." + sourceTable + " T1 GROUP BY [維度欄位];\n" 
					+ "select [維度欄位], count(1) cnt FROM " + raw + "." + tableName + " T1 GROUP BY [維度欄位];\n\n" 
					+ "-- 資料抽樣\n"
					+ "SELECT *\n" 
					+ "FROM " + raw + "." + sourceTable + " T1\n"
					+ "WHERE [PK欄位] = '' ;\n\n" 
					+ "SELECT *\n" + "FROM " + raw + "." + tableName + " T1\n" 
					+ "WHERE [PK欄位] = '' ;\n";

			return rs;
//		} else {
//			String dmRs = "-- 總筆數\n" 
//					+ "select count(1) cnt from " + raw + "." + sourceTable + " ;\n"
//					+ "select count(1) cnt from " + raw + "." + tableName + " ;\n\n" ;
//			String verifySum = 
//					 "-- 數值加總\n"
//					+ "Select \n" + rcptColLogic + "\n"
//					+ "FROM " + raw + "." + sourceTable + " T1 ;\n\n" 
//					+ "Select \n" + rcptColLogic + "\n"
//					+ "FROM " + raw + "." + tableName + " T1 ;\n" + "\n" ;
//			dmRs += StringUtils.isBlank(rcptColLogic) ? "-- 無數值欄位，故不需驗證數值欄位的加總--\n\n" : verifySum;
//			dmRs += "-- 維度分群加總\n"
//					+ "select [維度欄位], count(1) cnt FROM " + raw + "." + sourceTable + " T1 GROUP BY [維度欄位];\n" 
//					+ "select [維度欄位], count(1) cnt FROM " + raw + "." + tableName + " T1 GROUP BY [維度欄位];\n\n" 
//					+ "-- 資料抽樣\n"
//					+ "SELECT *\n" 
//					+ "FROM " + raw + "." + sourceTable + " T1\n"
//					+ "WHERE [PK欄位] = '' ;\n\n" 
//					+ "SELECT *\n" + "FROM " + raw + "." + tableName + " T1\n" 
//					+ "WHERE [PK欄位] = '' ;\n";
//
//			return dmRs;
//		}
	}

}
