package gss.Write;

import java.util.Map;

import gss.Tools.FileTools;

/**
 * 產出測試時所需SQL
 * @author nicole_tsou
 *
 */
public class WriteToRCPT {
	private static final String className = WriteToRCPT.class.getName();
	
	public static void run(String outputPath, Map<String, String> mapProp, String tableName, String odsTableName,
			String rcptODSColLogic, String rcptColLogic) throws Exception {
		try {
			String raw = mapProp.get("hadoop.raw.dbname");
			
			String sql = "-- 總筆數\n" 
					+ "select count(1) cnt from " + raw + "." + odsTableName + " ;\n"
					+ "select count(1) cnt from " + raw + "." + tableName + " ;\n\n" 
					+ "-- 數值加總\n"
					+ "Select \n" + rcptODSColLogic.substring(0,rcptODSColLogic.length() - 2) + "\n"
//					+ "	sum(cast(POLICY_K_SEQ as SMALLINT)) as POLICY_K_SEQ ,\n"
//					+ "	sum(cast(PAY_FRQ as SMALLINT)) as PAY_FRQ ,\n"
//					+ "	sum(cast(INS_AMT as INTEGER)) as INS_AMT ,\n"
//					+ "	sum(cast(EXCHANGE_RATE as DECIMAL(8,4))) as EXCHANGE_RATE ,\n"
//					+ "	sum(cast(AMT as INTEGER)) as AMT \n" 
					+ "FROM " + raw + "." + odsTableName + " T1 ;\n\n" 
					+ "Select \n" + rcptColLogic.substring(0,rcptColLogic.length() - 2) + "\n"
//					+ "	sum(POLICY_K_SEQ) as POLICY_K_SEQ,\n"
//					+ "	sum(PAY_FRQ) as PAY_FRQ,\n" 
//					+ "	sum(INS_AMT) as INS_AMT,\n"
//					+ "	sum(EXCHANGE_RATE) as EXCHANGE_RATE,\n"
//					+ "	sum(AMT) as AMT\n" 
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

			FileTools.createFile(outputPath, "RCPT", "sql", sql);
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}
}
