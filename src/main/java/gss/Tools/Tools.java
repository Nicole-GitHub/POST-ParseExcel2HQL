package gss.Tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class Tools {
	private static final String className = Tools.class.getName();

	public static void main(String arg[]) {
		System.out.println(getNowForROCYear("yyyy/MM/dd"));
	}

	public static String getNOW(String format) {
		Calendar c = Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat(format);
	    String dateToStr = dateFormat.format(c.getTime());
	    
		return dateToStr;
	}
	

	public static String getNowForROCYear(String format) {
		String now = getNOW(format);
		now = (Integer.parseInt(now.substring(0,4)) - 1911) + now.substring(4);
	    
		return now;
	}

	/**
	 * 調整最後輸出的partition順序(需與Layout頁籤的partition欄位相同)
	 * @param partitionList
	 * @param rsSelectPartitionList
	 * @param rsTargetSelectCols
	 */
	public static String tunePartitionOrder(String[] partitionList, List<Map<String, String>> rsSelectPartitionList) {
		String rsTargetSelectCols = "";
		// 確認最後輸出的partition順序需與Layout頁籤的partition欄位相同
		for (String str : partitionList) {
			for (Map<String, String> rsSelectPartition : rsSelectPartitionList) {
				str = str.trim();
				if (rsSelectPartition.get("Col").toString().equalsIgnoreCase(str)) {
					rsTargetSelectCols += rsSelectPartition.get("Script").toString();
					break;
				}
			}
		}
		return rsTargetSelectCols;
	}
	
}
