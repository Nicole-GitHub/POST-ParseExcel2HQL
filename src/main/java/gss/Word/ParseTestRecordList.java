package gss.Word;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.usermodel.Range;

public class ParseTestRecordList {
	private static final String className = ParseTestRecordList.class.getName();
	
//	public static void main (String arg[]) throws Exception {
//		readwriteWord();
//	}

	/**
	 * 整理測試紀錄各項目的編號最大值
	 */
	public static Map<String,Integer> getTestRecordMaxNum(String inputPath) throws FileNotFoundException {

//		String inputPath = "C:\\SVN\\dw2209\\DOCUMENT\\5-ST\\測試紀錄清單.doc";
		FileInputStream in = null;
		HWPFDocument hdt = null;
		Map<String,Integer> map = new HashMap<String,Integer>();
		
		try {
			in = new FileInputStream(new File(inputPath));
			hdt = new HWPFDocument(in);
		
			// 读取word文本内容
			Range range = hdt.getRange();
			String[] strArry = range.text().split("\007");
			for(int i = 7 ; i < strArry.length ; i++) {
				if(StringUtils.isNotBlank(strArry[i]) && i%7 == 1) {
					int last_ = strArry[i].lastIndexOf("_");
					String title = strArry[i].substring(0,last_);
					int num = Integer.parseInt(strArry[i].substring(last_+1).toString());
					if(map.get(title) == null || Integer.parseInt(map.get(title).toString()) < num)
						map.put(title, num);
				}
			}
//			System.out.println("range.text():"+range.text());
	
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

		System.out.println(className + " DONE!");
		return map;
	}

}
