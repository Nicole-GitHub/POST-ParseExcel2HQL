package gss.TableLayout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * 整合資料關聯與欄位處理邏輯資訊
 * @author Nicole
 *
 */
public class BuildLogic {
	private static final String className = BuildLogic.class.getName();
	
	public static List<Map<String, String>> run (List<Map<String, String>> tableMapList, List<Map<String, String>> colsMapList , Map<String, String> mapProp
			, Map<String, String> layoutMap, String partition, String fileName) throws Exception {

		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		Map<String, String> map = new HashMap<String, String>();
		String tableType = "D" + layoutMap.get("TableName").substring(5, 6);

		String rawDBName = mapProp.get("hadoop.raw.dbname");
		String tmpDBName = mapProp.get("hadoop.tmp.dbname");
		try {
			for (Map<String, String> mapTable : tableMapList) {
				for (Map<String, String> mapCols : colsMapList) {
					if (mapTable.get("Target").toString().equalsIgnoreCase(mapCols.get("Target").toString())) {
						String tableDBName = mapTable.get("Target").contains("{raw}.") ? rawDBName : tmpDBName;
						String target = mapTable.get("Target").substring(mapTable.get("Target").indexOf(".") + 1);
						String tmpTableName = tableDBName + "." + layoutMap.get("TableName")
								+ (!"TARGET".equalsIgnoreCase(target) ? "_" + target : "");
						String sql = "INSERT OVERWRITE TABLE " + tmpTableName + " \n";
						if ("TARGET".equalsIgnoreCase(target))
							sql += "PARTITION(" + (StringUtils.isBlank(partition) ? "" : partition + ",")
									+ " batchid) \n";
						sql += mapCols.get("Select") + " \n" + mapTable.get("FromWhere")
								+ (!StringUtils.isBlank(mapCols.get("Group")) ? " \n" + mapCols.get("Group") : "")
								+ (!StringUtils.isBlank(mapCols.get("Order")) ? " \n" + mapCols.get("Order") : "")
								+ ";";
						sql = sql.replace("{tmp}.", tmpDBName + "." + layoutMap.get("TableName") + "_")
								.replace("{raw}", rawDBName);
						String createSql = mapCols.get("CreateSql").toString().replace("{tmp}.",
								tmpDBName + "." + layoutMap.get("TableName") + "_");
						String hqlName = tableType + "_" + mapTable.get("Step");

						map = new HashMap<String, String>();
						map.put("Folder", fileName);
						map.put("HQLName", hqlName);
						map.put("SQL", sql);
						mapList.add(map);

						// Target Table 的 Create Script 用 Layout 頁籤產
						if (!StringUtils.isBlank(createSql)) {
							map = new HashMap<String, String>();
							map.put("Folder", fileName);
							map.put("HQLName", hqlName + "_create");
							map.put("SQL", createSql);
							mapList.add(map);
						}
					}
				}
			}
		} catch (Exception ex) {
			throw new Exception(className + " Error: \n" + ex);
		}

		System.out.println(className + " Done!");
		return mapList;
	}
}
