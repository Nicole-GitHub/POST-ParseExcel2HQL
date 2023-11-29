package gss.Word;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.usermodel.Range;

//======================生成新文档的方式：：==========================

/**
 * 实现java用poi实现对word读取和修改操作
 * 
 * @author fengcl
 *
 */
public class Sample {
	
	public static void main (String arg[]) throws Exception {
		String filePath = "C:\\Users\\nicole_tsou\\Dropbox\\POST\\ETL\\ETL 第二階\\程式\\_TESTING\\";
		String inputPath1 = filePath + "Sample收載-測試紀錄-ETL_PMM_CM_01.doc";
		String inputPath2 = filePath + "Sample梳理-測試紀錄-ETL_PMM_PI_63.doc";

		List<Map<String, String>> mapList = RunParseTestInfo.run();
		for(Map<String, String> map : mapList) {
			if("收載".equals(map.get("_type")))
				readwriteWord(inputPath1, filePath, map);
			else
				readwriteWord(inputPath2, filePath, map);
		}
		
	}

	/**
	 * 实现对word读取和修改操作
	 * 
	 * @param filePath word模板路径和名称
	 * @param map      待填充的数据，从数据库读取
	 */
	public static void readwriteWord (String inputPath, String filePath, Map<String, String> map) throws FileNotFoundException {

		FileInputStream in = null;
		HWPFDocument hdt = null;

		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		FileOutputStream out = null;
		
		try {
			in = new FileInputStream(new File(inputPath));
			hdt = new HWPFDocument(in);
		
			// 读取word文本内容
			Range range = hdt.getRange();
//			System.out.println("range.text():"+range.text());
			
			// 替换文本内容
			for (Map.Entry<String, String> entry : map.entrySet()) {
					range.replaceText(entry.getKey(), entry.getValue());
			}
			
			String targetTableEName = map.get("[Replace_TargetTable]");
			String targetTableCName = map.get("[Replace_規格描述]");
			String outputPath = filePath + "DONE\\" + targetTableEName + "-" + targetTableCName + "\\" + map.get("_testFileName") + ".doc"; 
	
//			System.out.println("outputPath:"+outputPath);
		
			out = new FileOutputStream(outputPath, true);
			hdt.write(ostream);
			// 输出字节流
			out.write(ostream.toByteArray());
			out.close();
			ostream.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

}
