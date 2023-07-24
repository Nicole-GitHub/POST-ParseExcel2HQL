package gss.ETLCode;

public class flow {

	public static String get_def_noExport() {

		String rs = "1:BEFORE_C03_ExportDate2File.sh\n"
				+ "2:BEFORE_C04_FinishData.sh\n"
				+ "3:FILE_UPLOAD.sh\n"
				+ "4:TRUNCATE_DW.sh\n"
				+ "5:DW_L07_LoadDW.hql\n"
				+ "6:DW_L08_LoadDW_TMP.hql\n"
				+ "7:FINISH.hql\n";
				
		return rs;
	}

	public static String get_def_all() {

		String rs = "1:BEFORE_C03_ExportDate2File.sh\n"
				+ "2:BEFORE_C04_FinishData.sh\n"
				+ "3:FILE_UPLOAD.sh\n"
				+ "4:TRUNCATE_DW.sh\n"
				+ "5:DW_L07_LoadDW.hql\n"
				+ "6:DW_L08_LoadDW_TMP.hql\n"
				+ "7:DW_EXPORT_2SQL.sh\n"
				+ "8:FINISH.hql\n";
		
		return rs;
	}
}
