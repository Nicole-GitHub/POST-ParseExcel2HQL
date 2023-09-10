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
				+ "set hivevar:SRC2_BACKUP_DW="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
				+ "set hivevar:DES1="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- 將T的最終結果直接寫入DES1中 \n"
				+ "INSERT OVERWRITE TABLE ${hivevar:DES1}\n"
				+ (!StringUtils.isBlank(partition) ? "PARTITION(" + partition + ") \n" : "")
				+ "select * \n"
				+ "from ${hivevar:SRC1_L}\n"
				+ ";" ;

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
				+ "set hivevar:SRC2_BACKUP_DW="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
				+ "set hivevar:DES1="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
		
		return rs;
	}
}
