package gss.ETLCode;

import java.util.Map;

public class flow {

//	public static String get_def_noExport(Map<String, String> mapProp) {
//
//		String rs = "";
//
//		if("1".equals(mapProp.get("runType"))) {
//			rs = "1:BEFORE_C03_ExportDate2File.sh\n"
//				+ "2:BEFORE_C04_FinishData.sh\n"
//				+ "3:FILE_UPLOAD.sh\n"
//				+ "4:TRUNCATE_DW.sh\n"
//				+ "5:DW_L07_LoadDW.hql\n"
//				+ "6:DW_L08_LoadDW_TMP.hql\n"
//				+ "7:FINISH.hql\n";
//		} else {
//			rs = "1:BEFORE_C01_Run.hql\n"
//				+ "2:BEFORE_C02_Check.hql\n"
//				+ "3:BEFORE_C03_ExportDate2File.sh\n"
//				+ "4:BEFORE_C04_FinishData.sh\n"
//				+ "5:DM_T01.hql\n"
//				+ "6:BACKUP_DM.hql\n"
//				+ "7:TRUNCATE_DM.sh\n"
//				+ "8:DM_L02_LoadDM.hql\n"
//				+ "9:DM_L03_LoadDM_TMP.hql\n";
//			
//			if("Y".equalsIgnoreCase(mapProp.get("exportfile")))
//				rs += "10:EXPORTFILE.sh\n"
//					+ "11:FINISH.hql\n";
//			else
//				rs += "10:FINISH.hql\n";
//		}
//		
//		return rs;
//	}

//	public static String get_def_all(Map<String, String> mapProp) {
//
//		String rs = "";
//		
//		if("1".equals(mapProp.get("runType"))) {
//			rs = "1:BEFORE_C03_ExportDate2File.sh\n"
//				+ "2:BEFORE_C04_FinishData.sh\n"
//				+ "3:FILE_UPLOAD.sh\n"
//				+ "4:TRUNCATE_DW.sh\n"
//				+ "5:DW_L07_LoadDW.hql\n"
//				+ "6:DW_L08_LoadDW_TMP.hql\n"
//				+ "7:DW_EXPORT_2SQL.sh\n"
//				+ "8:FINISH.hql\n";
//		} else {
//			rs = "1:BEFORE_C01_Run.hql\n"
//				+ "2:BEFORE_C02_Check.hql\n"
//				+ "3:BEFORE_C03_ExportDate2File.sh\n"
//				+ "4:BEFORE_C04_FinishData.sh\n"
//				+ "5:DM_T01.hql\n"
//				+ "6:BACKUP_DM.hql\n"
//				+ "7:TRUNCATE_DM.sh\n"
//				+ "8:DM_L02_LoadDM.hql\n"
//				+ "9:DM_L03_LoadDM_TMP.hql\n"
//				+ "10:TRUNCATE_MSSQL.sh\n"
//				+ "11:DM_EXPORT_2SQL.sh\n";
//
//			if("Y".equalsIgnoreCase(mapProp.get("exportfile")))
//				rs += "12:EXPORTFILE.sh\n"
//					+ "13:FINISH.hql\n";
//			else
//				rs += "12:FINISH.hql\n";
//			
//		}
//		
//		return rs;
//		
//	}
}
