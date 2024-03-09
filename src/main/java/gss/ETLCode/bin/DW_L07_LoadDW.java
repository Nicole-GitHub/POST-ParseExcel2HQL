package gss.ETLCode.bin;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class DW_L07_LoadDW {

	public static String getHQL(String partition, Map<String, String> mapProp, String selectStr, String sumColLogic,
			String sumODSColLogic, String whereSumCol, String tableName, String odsTableName) {

		String raw = mapProp.get("hadoop.raw.dbname");
		String tmp = mapProp.get("hadoop.tmp.dbname");
		String meta = mapProp.get("hadoop.meta.dbname");
		boolean runType = "1".equals(mapProp.get("runType"));
		String func_name = runType ? "_BackDW" : "_merge2DMTMP";
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW=post1_post_poc_std.SYS_RUN_NOW;\n"
				+ "set hivevar:DATE_CTRL=post1_post_poc_raw.T_CMMM_CCSYM_DTL;\n"
				+ "set hivevar:RSLT="+ tmp +"."+tableName+"_result;\n"
				+ (runType ? "set hivevar:SRC1_L07="+ raw +"."+odsTableName+";\n"
						: "set hivevar:SRC1="+ raw +".[***SourceTableName***];\n")
				+ (runType ? "set hivevar:SRC2_L07="+ meta +"."+odsTableName+"_done_files;\n"
						: "set hivevar:SRC2_T="+ meta +".[***ODSTableName***]_done_files;\n")
				+ (runType ? "set hivevar:DES1="+ raw +"."+tableName+";\n"
						:"set hivevar:DES1_TMP="+ tmp +".tmp_"+tableName+";\n")
				+ (runType ? "set hivevar:SRC3_L07="+ tmp +".BKD_"+tableName+";\n"
						:"set hivevar:SRC3_T="+ raw +"."+tableName+";\n")
				+ "\n"
				+ "set hivevar:FUNC_NAME1="+tableName+func_name+"_TotalDataCount;\n"
				+ "set hivevar:FUNC_NAME2="+tableName+func_name+"_SumNumData;\n"
				+ "set hivevar:FUNC_NAME3="+tableName+func_name+"_GroupSum;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"1;\n"
				+ "set hivevar:TMP2=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"2;\n"
				+ "set hivevar:TMP3=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"3;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "\n"
				+ "-- main\n";
		
			rs += runType ? 
				"INSERT OVERWRITE TABLE ${hivevar:DES1}\n"
				+ (!StringUtils.isBlank(partition) ? "PARTITION(" + partition + ") \n" : "")
				: "-- 先用target table schema建立後再用insert into方式塞資料，以免發生schema不合的情況\n"
				+ "drop table if exists ${hivevar:DES1_TMP};\n"
				+ "create table ${hivevar:DES1_TMP} like ${hivevar:SRC3_T};\n"
				+ "INSERT into ${hivevar:DES1_TMP}\n";
			
			rs += "Select \n" + selectStr + "\n"
				+ "FROM ${hivevar:"+ (runType ? "SRC1_L07" : "SRC1") +"} T1 \n";
			rs += !StringUtils.isBlank(partition) ? 
				"	join (select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') b\n"
				+ "	on T1." + partition + " = b.ymds\n" : "";
			rs += ";\n"
				+ "\n"
				+ "\n"
				+ "-- verification 總筆數\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) as row_count\n"
				+ "from (\n"
				+ "	select des_cnt,src_row\n"
				+ "	from (\n"
				+ "		select count(1) as des_cnt\n"
				+ "		from ${hivevar:"+ (runType ? "DES1" : "DES1_TMP") +"} a1 \n";
			rs += !StringUtils.isBlank(partition) ?
				"			join (select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}') run\n"
				+ "			on a1." + partition + " = run.ymds\n" : "";
			rs += "	) a\n"
				+ "	, (\n"
				+ "		select sum(total_row) as src_row\n"
				+ "		from ${hivevar:"+ (runType ? "SRC2_L07" : "SRC2_T") +"}\n"
				+ "	) b\n"
				+ ")H\n"
				+ "where des_cnt = src_row\n"
				+ ";\n"
				+ "\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME1}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when row_count > 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n"
				+ "\n";
			String verifySum = 
				 "-- verification 數值欄位加總\n"
				+ "drop table if exists ${hivevar:TMP2};\n"
				+ "create table ${hivevar:TMP2}\n"
				+ "as\n";
			verifySum += !StringUtils.isBlank(partition) ? 
				"with run as (\n"
				+ "	select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}'\n"
				+ ")\n" : "";
			verifySum += "select count(1) as row_count\n"
				+ "from (\n"
				+ "	select * \n"
				+ "	from (\n"
				+ "		select \n" + sumColLogic + "\n"
				+ "		from ${hivevar:"+ (runType ? "DES1" : "DES1_TMP") +"} a1 ";
			verifySum += !StringUtils.isBlank(partition) ? "join run on a1." + partition + " = run.ymds\n" : "\n";
			verifySum += "	) a\n"
				+ "	, (\n"
				+ "		select \n" + sumODSColLogic + "\n"
				+ "		from ${hivevar:"+ (runType ? "SRC1_L07" : "SRC1") +"} a1 ";
			verifySum += !StringUtils.isBlank(partition) ? "join run on a1." + partition + " = run.ymds\n": "\n";
			verifySum += "	) b\n"
				+ "	where " + whereSumCol + "\n"
				+ " )H\n"
				+ ";\n"
				+ "\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME2}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when row_count > 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP2}\n"
				+ ";\n"
				+ "\n"
				+ "\n";
			rs += StringUtils.isBlank(sumColLogic) ? "-- 無數值欄位，故不需驗證數值欄位的加總--\n\n" : verifySum;
			rs += "-- verification 維度分群加總\n"
				+ "drop table if exists ${hivevar:TMP3};\n"
				+ "create table ${hivevar:TMP3}\n"
				+ "as\n";
			rs += !StringUtils.isBlank(partition) ? 
				"with run as (\n"
				+ "	select SUBSTRING(ymds,1,6) ymds from ${hivevar:RUN_NOW} where trim(tablenm) = '${hivevar:Run_TableName}'\n"
				+ ")\n" : "";
			rs += "select count(1) as err_row_count\n"
				+ "from (\n"
				+ "	select " + (!StringUtils.isBlank(partition) ? "a." + partition + "," : "") 
				+ "a.[維度欄位],a.des_cnt,b.src_row\n"
				+ "	from (\n"
				+ "		select " + (!StringUtils.isBlank(partition) ? partition + "," : "") 
				+ " [維度欄位], count(1) des_cnt\n"
				+ "		from ${hivevar:"+ (runType ? "DES1" : "DES1_TMP") +"} a1 ";
			rs += !StringUtils.isBlank(partition) ? "join run on a1." + partition + " = run.ymds\n" : "\n";
			rs += "		group by " + (!StringUtils.isBlank(partition) ? partition+ "," : "") + "[維度欄位]\n";
			rs += "	) a\n"
				+ "	full outer join (\n"
				+ "		select " + (!StringUtils.isBlank(partition) ? partition + "," : "") 
				+ " [維度欄位], count(1) src_row\n"
				+ "		from ${hivevar:"+ (runType ? "SRC1_L07" : "SRC1") +"} a1 ";
			rs += !StringUtils.isBlank(partition) ? "join run on a1." + partition + " = run.ymds\n" : "\n";
			rs += "		group by " + (!StringUtils.isBlank(partition) ? partition+ "," : "") + "[維度欄位]\n";
			rs += "	) b\n";
			rs += !StringUtils.isBlank(partition) ? "\ton a."+partition+" = b."+partition+"\n\tAND " : "\ton ";
			rs += "a.[維度欄位] = b.[維度欄位]) H\n"
				+ "where des_cnt <> src_row or [維度欄位] is null\n"
				+ ";\n"
				+ "\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   ${hivevar:BATCHID},\n"
				+ "   '${hivevar:FUNC_NAME3}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when err_row_count > 0 then 1\n"
				+ "      else 0\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP3}\n"
				+ ";\n";
		
		return rs;
	}
	

	public static String getVAR(Map<String, String> mapProp, String tableName, String odsTableName) {
		
		String raw = mapProp.get("hadoop.raw.dbname");
		String tmp = mapProp.get("hadoop.tmp.dbname");
		String meta = mapProp.get("hadoop.meta.dbname");
		boolean runType = "1".equals(mapProp.get("runType"));
		String func_name = runType ? "_BackDW" : "_merge2DMTMP";
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:BATCHID=20230329000000;\n"
				+ "set hivevar:Run_TableName="+tableName+";\n"
				+ "set hivevar:RUN_NOW=post1_post_poc_std.SYS_RUN_NOW;\n"
				+ "set hivevar:DATE_CTRL=post1_post_poc_raw.T_CMMM_CCSYM_DTL;\n"
				+ "set hivevar:RSLT="+ tmp +"."+tableName+"_result;\n"
				+ (runType ? "set hivevar:SRC1_L07="+ raw +"."+odsTableName+";\n"
						: "set hivevar:SRC1="+ raw +"."+tableName+";\n")
				+ (runType ? "set hivevar:SRC2_L07="+ meta +"."+odsTableName+"_done_files;\n"
						: "set hivevar:SRC2_T="+ meta +".[***odsTableName***]_done_files;\n")
				+ (runType ? "set hivevar:DES1="+ raw +"."+tableName+";\n"
						:"set hivevar:DES1_TMP="+ tmp +".tmp_"+tableName+";\n")
				+ (runType ? "set hivevar:SRC3_L07="+ tmp +".BKD_"+tableName+";\n"
						:"set hivevar:SRC3_T="+ raw +"."+tableName+";\n")
				+ "\n"
				+ "set hivevar:FUNC_NAME1="+tableName+func_name+"_TotalDataCount;\n"
				+ "set hivevar:FUNC_NAME2="+tableName+func_name+"_SumNumData;\n"
				+ "set hivevar:FUNC_NAME3="+tableName+func_name+"_GroupSum;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"1;\n"
				+ "set hivevar:TMP2=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"2;\n"
				+ "set hivevar:TMP3=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"3;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n";
		
		return rs;
	}
	
	public static String getHQL_airflow(String partition, Map<String, String> mapProp, String selectStr, String sumColLogic,
			String sumODSColLogic, String whereSumCol, String tableName, String odsTableName) {

		String raw = mapProp.get("hadoop.raw.dbname");
		String tmp = mapProp.get("hadoop.tmp.dbname");
		String meta = mapProp.get("hadoop.meta.dbname");
		boolean runType = "1".equals(mapProp.get("runType"));
		String func_name = runType ? "_BackDW" : "_merge2DMTMP";
		
		String rs = "-----------------------------------------------------------------\n"
				+ "-- parameter list\n"
				+ "-----------------------------------------------------------------\n"
				+ "set hivevar:ACT_YM=202303;\n"
				+ "set hivevar:DATE_CTRL=post1_post_poc_raw.T_CMMM_CCSYM_DTL;\n"
				+ "set hivevar:RSLT="+ tmp +"."+tableName+"_result;\n"
				+ (runType ? "set hivevar:SRC1_L07="+ raw +"."+odsTableName+";\n"
						: "set hivevar:SRC1="+ raw +".[***SourceTableName***];\n")
				+ (runType ? "set hivevar:SRC2_L07="+ meta +"."+odsTableName+"_done_files;\n"
						: "set hivevar:SRC2_T="+ meta +".[***ODSTableName***]_done_files;\n")
				+ (runType ? "set hivevar:DES1="+ raw +"."+tableName+";\n"
						:"set hivevar:DES1_TMP="+ tmp +".tmp_"+tableName+";\n")
				+ (runType ? "set hivevar:SRC3_L07="+ tmp +".BKD_"+tableName+";\n"
						:"set hivevar:SRC3_T="+ raw +"."+tableName+";\n")
				+ "\n"
				+ "set hivevar:FUNC_NAME1="+tableName+func_name+"_TotalDataCount;\n"
				+ "set hivevar:FUNC_NAME2="+tableName+func_name+"_SumNumData;\n"
				+ "set hivevar:FUNC_NAME3="+tableName+func_name+"_GroupSum;\n"
				+ "set hivevar:KEY_NAME=return_code;\n"
				+ "set hivevar:LOGIC_NAME="+tableName+";\n"
				+ "set hivevar:TMP1=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"1;\n"
				+ "set hivevar:TMP2=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"2;\n"
				+ "set hivevar:TMP3=tmp_"+tableName+"_0"+(runType ? "7" : "1")+"3;\n"
				+ "-----------------------------------------------------------------\n"
				+ "\n"
				+ "\n"
				+ "-- main\n";
		
			rs += runType ? 
				"INSERT OVERWRITE TABLE ${hivevar:DES1}\n"
				+ (!StringUtils.isBlank(partition) ? "PARTITION(" + partition + ") \n" : "")
				: "-- 先用target table schema建立後再用insert into方式塞資料，以免發生schema不合的情況\n"
				+ "drop table if exists ${hivevar:DES1_TMP};\n"
				+ "create table ${hivevar:DES1_TMP} like ${hivevar:SRC3_T};\n"
				+ "INSERT into ${hivevar:DES1_TMP}\n";
			
			rs += "Select \n" + selectStr + "\n"
				+ "FROM ${hivevar:"+ (runType ? "SRC1_L07" : "SRC1") +"} T1 \n";
			rs += !StringUtils.isBlank(partition) ? 
				"where ACT_YM=${hivevar:ACT_YM}\n" : "";
			rs += ";\n"
				+ "\n"
				+ "\n"
				+ "-- verification 總筆數\n"
				+ "drop table if exists ${hivevar:TMP1};\n"
				+ "create table ${hivevar:TMP1}\n"
				+ "as\n"
				+ "select count(1) as row_count\n"
				+ "from (\n"
				+ "	select des_cnt,src_row\n"
				+ "	from (\n"
				+ "		select count(1) as des_cnt\n"
				+ "		from ${hivevar:"+ (runType ? "DES1" : "DES1_TMP") +"} a1 \n";
			rs += !StringUtils.isBlank(partition) ?
				"		where ACT_YM=${hivevar:ACT_YM}\n" : "";
			rs += "	) a\n"
				+ "	, (\n"
				+ "		select sum(total_row) as src_row\n"
				+ "		from ${hivevar:"+ (runType ? "SRC2_L07" : "SRC2_T") +"}\n"
				+ "	) b\n"
				+ ")H\n"
				+ "where des_cnt = src_row\n"
				+ ";\n"
				+ "\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   '${hivevar:FUNC_NAME1}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when row_count > 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP1}\n"
				+ ";\n"
				+ "\n";
			String verifySum = 
				 "-- verification 數值欄位加總\n"
				+ "drop table if exists ${hivevar:TMP2};\n"
				+ "create table ${hivevar:TMP2}\n"
				+ "as\n";
			verifySum += "select count(1) as row_count\n"
				+ "from (\n"
				+ "	select * \n"
				+ "	from (\n"
				+ "		select \n" + sumColLogic + "\n"
				+ "		from ${hivevar:"+ (runType ? "DES1" : "DES1_TMP") +"} a1 \n";
			verifySum += !StringUtils.isBlank(partition) ? 
				"		where ACT_YM=${hivevar:ACT_YM}\n" : "";
			verifySum += "	) a\n"
				+ "	, (\n"
				+ "		select \n" + sumODSColLogic + "\n"
				+ "		from ${hivevar:"+ (runType ? "SRC1_L07" : "SRC1") +"} a1 \n";
			verifySum += !StringUtils.isBlank(partition) ? 
				"		where ACT_YM=${hivevar:ACT_YM}\n" : "";
			verifySum += "	) b\n"
				+ "	where " + whereSumCol + "\n"
				+ " )H\n"
				+ ";\n"
				+ "\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   '${hivevar:FUNC_NAME2}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when row_count > 0 then 0\n"
				+ "      else 1\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP2}\n"
				+ ";\n"
				+ "\n"
				+ "\n";
			rs += StringUtils.isBlank(sumColLogic) ? "-- 無數值欄位，故不需驗證數值欄位的加總--\n\n" : verifySum;
			rs += "-- verification 維度分群加總\n"
				+ "drop table if exists ${hivevar:TMP3};\n"
				+ "create table ${hivevar:TMP3}\n"
				+ "as\n";
			rs += "select count(1) as err_row_count\n"
				+ "from (\n"
				+ "	select " + (!StringUtils.isBlank(partition) ? "a." + partition + "," : "") 
				+ "a.[維度欄位],a.des_cnt,b.src_row\n"
				+ "	from (\n"
				+ "		select " + (!StringUtils.isBlank(partition) ? partition + "," : "") 
				+ " [維度欄位], count(1) des_cnt\n"
				+ "		from ${hivevar:"+ (runType ? "DES1" : "DES1_TMP") +"} a1 \n";
			rs += !StringUtils.isBlank(partition) ? 
				"		where ACT_YM=${hivevar:ACT_YM}\n" : "";
			rs += "		group by " + (!StringUtils.isBlank(partition) ? partition+ "," : "") + "[維度欄位]\n";
			rs += "	) a\n"
				+ "	full outer join (\n"
				+ "		select " + (!StringUtils.isBlank(partition) ? partition + "," : "") 
				+ " [維度欄位], count(1) src_row\n"
				+ "		from ${hivevar:"+ (runType ? "SRC1_L07" : "SRC1") +"} a1 \n";
			rs += !StringUtils.isBlank(partition) ? 
				"		where ACT_YM=${hivevar:ACT_YM}\n" : "";
			rs += "		group by " + (!StringUtils.isBlank(partition) ? partition+ "," : "") + "[維度欄位]\n";
			rs += "	) b\n";
			rs += !StringUtils.isBlank(partition) ? "\ton a."+partition+" = b."+partition+"\n\tAND " : "\ton ";
			rs += "a.[維度欄位] = b.[維度欄位]) H\n"
				+ "where des_cnt <> src_row or [維度欄位] is null\n"
				+ ";\n"
				+ "\n"
				+ "-- 0 => success\n"
				+ "-- 1 => failure\n"
				+ "insert into table ${hivevar:RSLT}\n"
				+ "select\n"
				+ "   '${hivevar:LOGIC_NAME}',\n"
				+ "   '${hivevar:FUNC_NAME3}',\n"
				+ "   '${hivevar:KEY_NAME}',\n"
				+ "   'info',\n"
				+ "   case\n"
				+ "      when err_row_count > 0 then 1\n"
				+ "      else 0\n"
				+ "   end as rc,\n"
				+ "   current_timestamp() as create_time\n"
				+ "from ${hivevar:TMP3}\n"
				+ ";\n";
		
		return rs;
	}
	
}
