package gss.ETLCode.bin;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class BACKUP_DW {

//	public static String getHQL(String partition, Map<String, String> mapProp, String tableName, String type) {
//
//		String rs = "-----------------------------------------------------------------\n"
//				+ "-- parameter list\n"
//				+ "-----------------------------------------------------------------\n"
//				+ "set hivevar:BATCHID=20230329000000;\n"
//				+ "set hivevar:DES1_BACKUP_"+type+"="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
//				+ "set hivevar:Run_TableName="+tableName+";\n"
//				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
//				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
//				+ "set hivevar:SRC1_BACKUP_"+type+"="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
//				+ "set hivevar:FUNC_NAME="+tableName+"_Backup"+type+";\n"
//				+ "set hivevar:KEY_NAME=return_code;\n"
//				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
//				+ "set hivevar:TMP1=tmp_"+tableName+"_Backup"+type+";\n"
//				+ "-----------------------------------------------------------------\n"
//				+ "\n"
//				+ "-- 備份"+type+" Table\n";
//		rs += !StringUtils.isBlank(partition) ? "-- 有" : "-- 無" ;
//		rs += "Partiton\n"
//				+ "Create table ${hivevar:DES1_BACKUP_"+type+"}_${hivevar:BATCHID} AS \n"
//				+ "SELECT a.* FROM ${hivevar:SRC1_BACKUP_"+type+"} a \n";
//		rs += !StringUtils.isBlank(partition) ? 
//				"join (select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') b\n"
//				+ "on a."+partition+" = b.ymds" : "";
//		rs += "\n;\n\n"
//				+ "-- verification 確認備份後的筆數與來源一致\n"
//				+ "-- group SRC1與DES1的筆數後找count為2的值\n"
//				+ "drop table if exists ${hivevar:TMP1};\n"
//				+ "create table ${hivevar:TMP1}\n"
//				+ "as\n"
//				+ "select count(1) row_count\n"
//				+ "from (\n"
//				+ "	select row_count,count(1) cnt\n"
//				+ "	from (\n"
//				+ "		select count(1) as row_count\n"
//				+ "		from ${hivevar:DES1_BACKUP_"+type+"}_${hivevar:BATCHID}\n"
//				+ "		union all \n"
//				+ "		select count(1) as row_count\n"
//				+ "		from ${hivevar:SRC1_BACKUP_"+type+"} a \n";
//			rs += !StringUtils.isBlank(partition) ?
//				 "			join (select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') b\n"
//				+ "			on a." + partition + " = b.ymds\n" : "";
//			rs += "	) a1\n"
//				+ "	group by row_count\n"
//				+ "	having count(1) = 2\n"
//				+ ") b\n"
//				+ ";\n"
//				+ "\n"
//				+ "-- 若tmp1有資料則表示兩個檔案的筆數是一致的\n"
//				+ "-- 0 => success\n"
//				+ "-- 1 => failure\n"
//				+ "insert into table ${hivevar:RSLT}\n"
//				+ "select\n"
//				+ "   '${hivevar:LOGIC_NAME}',\n"
//				+ "   ${hivevar:BATCHID},\n"
//				+ "   '${hivevar:FUNC_NAME}',\n"
//				+ "   '${hivevar:KEY_NAME}',\n"
//				+ "   'info',\n"
//				+ "   case\n"
//				+ "      when row_count > 0 then 0\n"
//				+ "      else 1\n"
//				+ "   end as rc,\n"
//				+ "   current_timestamp() as create_time\n"
//				+ "from ${hivevar:TMP1}\n"
//				+ ";\n";
//			
//		return rs;
//	}

//	public static String getVAR(Map<String, String> mapProp, String tableName, String type) {
//
//		String rs = "-----------------------------------------------------------------\n"
//				+ "-- parameter list\n"
//				+ "-----------------------------------------------------------------\n"
//				+ "set hivevar:BATCHID=20230329000000;\n"
//				+ "set hivevar:DES1_BACKUP_"+type+"="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
//				+ "set hivevar:Run_TableName="+tableName+";\n"
//				+ "set hivevar:RUN_NOW="+mapProp.get("hadoop.std.dbname")+".SYS_RUN_NOW;\n"
//				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
//				+ "set hivevar:SRC1_BACKUP_"+type+"="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
//				+ "set hivevar:FUNC_NAME="+tableName+"_Backup"+type+";\n"
//				+ "set hivevar:KEY_NAME=return_code;\n"
//				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
//				+ "set hivevar:TMP1=tmp_"+tableName+"_Backup"+type+";\n"
//				+ "-----------------------------------------------------------------\n"
//				+ "\n";
//		
//		return rs;
//	}
	
	public static String getHQL_airflow(String partition, Map<String, String> mapProp, String tableName, String type) {

		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:ACT_YM=202303;\n"
				+ "set hivevar:DES1_BACKUP_"+type+"="+mapProp.get("hadoop.tmp.dbname")+".BKD_"+tableName+";\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+tableName+"_result;\n"
				+ "set hivevar:SRC1_BACKUP_"+type+"="+mapProp.get("hadoop.raw.dbname")+"."+tableName+";\n"
				+ "set hivevar:FUNC_NAME="+tableName+"_Backup"+type+";\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_Backup"+type+";\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "-- 備份"+type+" Table\n";
		rs += !StringUtils.isBlank(partition) ? "-- 有" : "-- 無" ;
		rs += "Partiton\n"
				+ "Create table ${hivevar:DES1_BACKUP_"+type+"}_${hivevar:BATCHID} AS \n"
				+ "SELECT a.* FROM ${hivevar:SRC1_BACKUP_"+type+"} a \n";
		rs += !StringUtils.isBlank(partition) ? 
				"where a."+partition+"=${hivevar:ACT_YM}" : "";
		rs += "\n;\n\n"
				+ "-- verification 確認備份後的筆數與來源一致\n"
				+ "-- group SRC1與DES1的筆數後找count為2的值\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) row_count\n"
				+ "from (\n"
				+ "	select row_count,count(1) cnt\n"
				+ "	from (\n"
				+ "		select count(1) as row_count\n"
				+ "		from ${hivevar:DES1_BACKUP_"+type+"}_${hivevar:BATCHID}\n"
				+ "		union all \n"
				+ "		select count(1) as row_count\n"
				+ "		from ${hivevar:SRC1_BACKUP_"+type+"} a \n";
			rs += !StringUtils.isBlank(partition) ?
				 "		where a."+partition+"=${hivevar:ACT_YM}\n" : "";
			rs += "	) a1\n"
				+ "	group by row_count\n"
				+ "	having count(1) = 2\n"
				+ ") b\n"
				+ ";\n"
				+ "\n"
				+ "-- 若tmp1有資料則表示兩個檔案的筆數是一致的\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   '${hivevar:FUNC_NAME}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when row_count > 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n";
			
		return rs;
	}
}
