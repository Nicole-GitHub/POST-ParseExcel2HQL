package gss.ETLCode;

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
	public static String getHQL(String raw, String rcptODSColLogic, String rcptColLogic, String odsTableName,
			String tableName) {

		String rs = "-- 總筆數\n" 
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
				+ "WHERE POLICY_NO = '' ;\n";
		
		return rs;
	}

}
