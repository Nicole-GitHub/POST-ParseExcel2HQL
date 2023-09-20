package gss.ETLCode.bin;

import java.util.Map;

public class ODS_L06_LoadODS {

	public static String getHQL(Map<String, String> mapProp, String selectStr, String tableName,
			boolean hasChineseForTable) {
		
		String ods_tableName = tableName;
		String t_tableName = tableName.replace("ODS_", "T_");
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+t_tableName+"_result;\n"
				+ "set hivevar:SRC1_L06="+mapProp.get("hadoop.meta.dbname")+"."+ods_tableName+"_data_files;\n"
				+ "set hivevar:SRC2_L06="+mapProp.get("hadoop.meta.dbname")+"."+ods_tableName+"_done_files;\n"
				+ "set hivevar:DES1_L06="+mapProp.get("hadoop.raw.dbname")+"."+ods_tableName+";\n"
				+ "set hivevar:FUNC_NAME="+ods_tableName+"_GetODS;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+t_tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+ods_tableName+"_061;\n";
			rs += hasChineseForTable ? "set hivevar:TMP2=tmp_"+ods_tableName+"_062; \n" : "";
			rs += "-----------------------------------------------------------------\n"
				+ "\n"
				+ "\n";
			
			rs += hasChineseForTable ? 
					"-- 因有中文欄位，將line先encode成big5\n"
					+ "drop table if exists ${hivevar:TMP2};\n"
					+ "create table ${hivevar:TMP2}\n"
					+ "as\n"
					+ "SELECT ENCODE(line,'BIG5') AS line\n"
					+ "FROM ${hivevar:SRC1_L06}\n"
					+ ";\n" 
					: "";
			
			rs += "\n"
				+ "-- Main logic\n"
				+ "INSERT OVERWRITE TABLE ${hivevar:DES1_L06}\n";
			rs += "SELECT \n" + selectStr.substring(0,selectStr.lastIndexOf(",")) + "\n"
				+ "FROM ${hivevar:" + (hasChineseForTable ? "TMP2" : "SRC1_L06") + "}\n"
				+ ";\n"
				+ "\n"
				+ "-- verification\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) as row_count\n"
				+ "from \n"
				+ "(\n"
				+ "	select des_cnt,src_row\n"
				+ "	from\n"
				+ "	(\n"
				+ "		select count(1) as des_cnt\n"
				+ "		from ${hivevar:DES1_L06}\n"
				+ "	) a\n"
				+ "	, \n"
				+ "	(\n"
				+ "		select sum(total_row) as src_row\n"
				+ "		from ${hivevar:SRC2_L06}\n"
				+ "	) b\n"
				+ ") H\n"
				+ "where des_cnt = src_row\n"
				+ ";\n"
				+ "\n"
				+ "-- 確認ODS筆數與done_files.total_row的值是否一致\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
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
	
	public static String getVAR(Map<String, String> mapProp, String tableName, boolean hasChineseForTable) {
		
		String ods_tableName = tableName;
		String t_tableName = tableName.replace("ODS_", "T_");
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:RSLT="+mapProp.get("hadoop.tmp.dbname")+"."+t_tableName+"_result;\n"
				+ "set hivevar:SRC1_L06="+mapProp.get("hadoop.meta.dbname")+"."+ods_tableName+"_data_files;\n"
				+ "set hivevar:SRC2_L06="+mapProp.get("hadoop.meta.dbname")+"."+ods_tableName+"_done_files;\n"
				+ "set hivevar:DES1_L06="+mapProp.get("hadoop.raw.dbname")+"."+ods_tableName+";\n"
				+ "set hivevar:FUNC_NAME="+ods_tableName+"_GetODS;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+t_tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+ods_tableName+"_061;\n";
			rs += hasChineseForTable ? "set hivevar:TMP2=tmp_"+ods_tableName+"_062; \n" : "";
			rs += "-----------------------------------------------------------------\n\n";
		
		return rs;
	}

}
