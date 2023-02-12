package gss.Tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Property {
	private static final String className = Property.class.getName();
	
	public static Map<String, String> getProperties(String path) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		Properties prop = new Properties();
		FileInputStream fis = null;

		try {
			fis = new FileInputStream(path + "config.properties");
			// 加載屬性
			prop.load(fis);

			// 取得所有鍵的列舉
			Enumeration<?> e = prop.propertyNames();
			while (e.hasMoreElements()) {
				// 取得下一個鍵
				String key = (String) e.nextElement();
				// 取得 properties 屬性值
				String value = prop.getProperty(key, "搜尋不到 " + key);
//				System.out.println(key + " = " + value);
				map.put(key, value);
			}

		} catch (IOException e) {
			throw new Exception(className + " getProperties Error: \n" + e);
		}finally {
			if(fis != null) fis.close();
		}

		return map;
	}
}
