package gss.ETLCode.bin;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class DM_L02_LoadDM {

	public static String getHQL(String partition, Map<String, String> mapProp, String tableName) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:SRC1_L="+mapProp.get("hadoop.tmp.dbname")+".tmp_"+tableName+";\n"
				+ "set hivevar:SRC2_BACKUP_DM="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
				+ "set hivevar:DES1="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- 將T的最終結果直接寫入DES1中 \n"
				+ "INSERT OVERWRITE TABLE ${hivevar:DES1}\n"
				+ (!StringUtils.isBlank(partition) ? "PARTITION(" + partition + ") \n" : "")
				+ "select * \n"
				+ "from ${hivevar:SRC1_L}\n";
		
			rs += StringUtils.isBlank(partition) ?
				"\n"
				+ "-- 無partition但又需保留舊資料時 \n"
				+ "UNION ALL \n"
				+ "\n"
				+ "select a.* \n"
				+ "FROM ${hivevar:SRC2_BACKUP_DM}_${hivevar:BATCHID} a\n"
				+ "join (select ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') run\n"
				+ "on [*** ACT_YM ***] <> run.ymds\n"
				+ ";" : ";";

		return rs;
	}

	public static String getVAR(Map<String, String> mapProp, String tableName) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
				+ "set hivevar:SRC1_L="+mapProp.get("hadoop.tmp.dbname")+".tmp_"+tableName+";\n"
				+ "set hivevar:SRC2_BACKUP_DM="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
				+ "set hivevar:DES1="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
		
		return rs;
	}
	
	public static String getHQL_airflow(String partition, Map<String, String> mapProp, String tableName) {
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:ACT_YM=202303;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:SRC1_L="+mapProp.get("hadoop.tmp.dbname")+".tmp_"+tableName+";\n"
				+ "set hivevar:SRC2_BACKUP_DM="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
				+ "set hivevar:DES1="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- 將T的最終結果直接寫入DES1中 \n"
				+ "INSERT OVERWRITE TABLE ${hivevar:DES1}\n"
				+ (!StringUtils.isBlank(partition) ? "PARTITION(" + partition + ") \n" : "")
				+ "select * \n"
				+ "from ${hivevar:SRC1_L}\n";
		
			rs += StringUtils.isBlank(partition) ?
				"\n"
				+ "-- 無partition但又需保留舊資料時 \n"
				+ "UNION ALL \n"
				+ "\n"
				+ "select a.* \n"
				+ "FROM ${hivevar:SRC2_BACKUP_DM}_${hivevar:BATCHID} a\n"
				+ "where ACT_YM <> ${hivevar:ACT_YM}\n"
				+ ";" : ";";

		return rs;
	}
}
