package gss.ETLCode;

public class description {

	public static String get() {

		String rs = "\n"
				+ "-- PURPOSE: file > ods > dw.\n"
				+ "\n"
				+ "-- Parameter (Argument) liating:\n"
				+ "----------------------------------\n"
				+ "-- BATCHID:batchid, job id.\n"
				+ "-- TMP_DB:temp DB.\n"
				+ "-- META_DB:META DB;\n"
				+ "-- RAW_DB:RAW_DATA_DB\n"
				+ "-- TAR_TABLE:TARGET TABLE NAME\n"
				+ "-- FIX_LENGTH:DATA LENGTH\n"
				+ "\n"
				+ "--RSLT若RC不為0就不會往下走嗎? 因為用了--is-check-query-result\n"
				+ "--HOW TO 做加密欄位\n"
				+ "--HQL的參數寫法\n"
				+ "----------------------------------\n\n";
		
		return rs;
	}

}
