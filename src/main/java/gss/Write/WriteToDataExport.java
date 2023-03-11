package gss.Write;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import gss.Tools.FileTools;

public class WriteToDataExport {
	private static final String className = WriteToDataExport.class.getName();

	public static void run(String outputPath, String fileName, List<Map<String, String>> layoutMapList, Map<String, String> mapProp)
			throws Exception {

		try {

	     	List<String> charTypeList = Arrays.asList(new String[] { "VARCHAR", "CHAR" });
			List<String> intTypeList = Arrays.asList(new String[] { "SMALLINT", "BIGINT", "INTEGER" });
	        
	     	// list的最後一筆位置
			int layoutMapListLastNum = layoutMapList.size() - 1;
			String tableNameLast = layoutMapList.get(layoutMapListLastNum).get("TableName").substring(7);
			String colENameStr = "", colLogic = "", colLogicStr = "", dateFormat = "yyyyMMdd", dtFormat = "yyyyMMddHHmmss";
			for (Map<String, String> layoutMap : layoutMapList) {
				if ("Detail".equals(layoutMap.get("MapType"))) {
					String colEName = layoutMap.get("ColEName").toString().toUpperCase();
					String colType = layoutMap.get("ColType").toString().toUpperCase();
					String colLen = layoutMap.get("ColLen").toString().toUpperCase();
					colENameStr += "\t"+colEName + ",\n";
					
					if (charTypeList.contains(colType))
						colLogic = "\trpad(nvl(" + colEName + ",'')," + colLen + ",' ') as " + colEName + ",\n";
					else if (intTypeList.contains(colType))
						colLogic = "\tlpad(nvl(" + colEName + ",'')," + colLen + ",' ') as " + colEName + ",\n";
					else if ("DATE".equals(colType))
						colLogic = "\tlpad(nvl(date_format(" + colEName + ", '" + dateFormat + "'), ''), " + colLen
								+ ", ' ') as " + colEName + ",\n";
					else if ("DATETIME".equals(colType))
						colLogic = "\tlpad(nvl(date_format(" + colEName + ", '" + dtFormat + "'), ''), " + colLen
								+ ", ' ') as " + colEName + ",\n";
					else if ("DECIMAL".equals(colType)) {
						String[] colLenArr = colLen.split(",");
						int colLenInt = Integer.parseInt(colLenArr[0]) + 1;
						colLogic = "\tlpad(nvl(format_number(" + colEName + ", " + colLenArr[1] + "),''), " + colLenInt
								+ ", ' ') as " + colEName + ",\n";
					}
					colLogicStr += colLogic;
				}
			}

			String raw = mapProp.get("hadoop.raw.dbname");
			colLogicStr = StringUtils.isBlank(colLogicStr) ? "" : colLogicStr.substring(0,colLogicStr.length() - 2);
			colENameStr = StringUtils.isBlank(colENameStr) ? "" : colENameStr.substring(0,colENameStr.length() - 2);
			String sql = "set hivevar:RAW_DB=" + raw + ";\n"
					+ "set hivevar:SRC1=T_IFTM_" + tableNameLast + ";\n"
					+ "set hivevar:ACT_YM=?;\n"
					+ "\n"
					+ "SET hivevar:KEY_STR=POST_DATA_EXPORT;\n"
					+ "SET hivevar:BATCHID=?;\n"
					+ "SET hivevar:TMP1=TMP_DATA_SET_" + tableNameLast + ";\n"
					+ "SET hivevar:TMP2=TMP_DATA_BLOCK_" + tableNameLast + ";\n"
					+ "SET hivevar:TMP3=TMP_HEADDER_FOOTER_INFO_" + tableNameLast + ";\n"
					+ "set hivevar:TMP4=TMP_HEADDER_FOOTER_MAXLEN_" + tableNameLast + ";\n"
					+ "SET hivevar:DES1=TMP_OUTPUT_" + tableNameLast + ";\n"
					+ "\n"
					+ "DROP TABLE IF EXISTS ${hivevar:TMP1};\n"
					+ "\n"
					+ "CREATE TABLE IF NOT EXISTS ${hivevar:TMP1} as \n"
					+ "select \n" + colLogicStr + "\n"
					+ "FROM ${hivevar:RAW_DB}.${hivevar:SRC1} a\n"
					+ "where a.ACT_YM = ${hivevar:ACT_YM}\n"
					+ ";\n"
					+ "\n"
					+ "DROP TABLE IF EXISTS ${hivevar:TMP2};\n"
					+ "\n"
					+ "CREATE TABLE IF NOT EXISTS ${hivevar:TMP2} as \n"
					+ "SELECT \n"
					+ "  '${hivevar:KEY_STR}' AS KEY_STR,\n"
					+ "  '${hivevar:BATCHID}' AS BATCHID,\n"
					+ "  2 AS BLOCK_TYPE,\n"
					+ "  row_number() over() AS RN,\n"
					+ "  concat(" + colENameStr + "\n"
					+ "    ) AS LINE\n"
					+ "FROM ${hivevar:TMP1}\n"
					+ ";\n"
					+ "\n"
					+ "\n"
					+ "DROP TABLE IF EXISTS ${hivevar:TMP3};\n"
					+ "\n"
					+ "CREATE TABLE IF NOT EXISTS ${hivevar:TMP3} as \n"
					+ "SELECT \n"
					+ "   KEY_STR,\n"
					+ "   BATCHID,\n"
					+ "   999 AS BLOCK_TYPE,\n"
					+ "   row_number() over() AS RN,\n"
					+ "   CONCAT(\n"
					+ "     '${hivevar:BATCHID}',\n"
					+ "     lpad(cast( COUNT(1) AS STRING),9,0)\n"
					+ "     ) AS LINE\n"
					+ "FROM ${hivevar:TMP2}\n"
					+ "GROUP BY KEY_STR, BATCHID\n"
					+ ";\n"
					+ "\n"
					+ "INSERT INTO TABLE ${hivevar:TMP3}\n"
					+ "SELECT \n"
					+ "   KEY_STR,\n"
					+ "   BATCHID,\n"
					+ "   row_number() over() AS RN,\n"
					+ "   1 AS BLOCK_TYPE,\n"
					+ "   '${hivevar:BATCHID}' AS LINE\n"
					+ "FROM ${hivevar:TMP2}\n"
					+ "GROUP BY KEY_STR, BATCHID\n"
					+ ";\n"
					+ "\n"
					+ "\n"
					+ "DROP TABLE IF EXISTS ${hivevar:TMP4};\n"
					+ "\n"
					+ "CREATE TABLE IF NOT EXISTS ${hivevar:TMP4} as \n"
					+ "select a.KEY_STR, a.BATCHID, a.BLOCK_TYPE, a.RN,\n"
					+ "   rpad(a.LINE, b.maxlen, ' ') as line\n"
					+ "from ${hivevar:TMP3} a, (SELECT max(LENGTH(LINE)) as maxlen FROM ${hivevar:TMP2}) as b\n"
					+ ";\n"
					+ "\n"
					+ "\n"
					+ "DROP TABLE IF EXISTS ${hivevar:DES1};\n"
					+ "\n"
					+ "CREATE TABLE IF NOT EXISTS ${hivevar:DES1} as \n"
					+ "   SELECT * FROM ${hivevar:TMP2}\n"
					+ "   UNION ALL\n"
					+ "   SELECT * FROM ${hivevar:TMP4}\n"
					+ ";\n"
					+ "\n"
					+ "\n"
					+ "SELECT line\n"
					+ "FROM ${hivevar:DES1}\n"
					+ "WHERE KEY_STR='${hivevar:KEY_STR}' \n"
					+ "	AND BATCHID='${hivevar:BATCHID}'\n"
					+ "ORDER BY KEY_STR, BATCHID, BLOCK_TYPE, RN\n"
					+ ";";

			FileTools.createFile(outputPath + fileName + "/", "DM_F01_" + tableNameLast, "hql", sql);
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
	}

}
