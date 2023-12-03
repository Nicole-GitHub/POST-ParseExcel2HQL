package gss.Tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;

public class FileTools {
	private static final String className = FileTools.class.getName();


	// 檔案路徑 名稱
	private static String filenameTemp;

	/**
	 * 建立檔案(保留舊資料)
	 * @param path
	 * @param fileName
	 * @param extension
	 * @param fileContent
	 * @return
	 * @throws Exception
	 */
	public static boolean createFileAppend(String path, String fileName, String extension, String fileContent)
			throws Exception {
		return createFile(path, fileName, extension, fileContent, true);
	}
	
	/**
	 * 建立檔案(不保留舊資料)
	 * @param path
	 * @param fileName
	 * @param extension
	 * @param fileContent
	 * @return
	 * @throws Exception
	 */
	public static boolean createFileNotAppend(String path, String fileName, String extension, String fileContent)
			throws Exception {
		return createFile(path, fileName, extension, fileContent, false);
	}
	/**
	 * 建立檔案
	 * 
	 * @param path			檔路徑
	 * @param fileName		檔名稱
	 * @param extension		副檔名
	 * @param fileContent	檔案內容
	 * @return 是否建立成功，成功則返回true
	 */
	private static boolean createFile(String path, String fileName, String extension, String fileContent,
			boolean append) throws Exception {
		String funcName = "createFile";
		Boolean bool = false;
		File file ;
		
		try {
			file = new File(path);
			if(!file.exists()) file.mkdirs();
			
			filenameTemp = path + fileName + "." + extension;// 檔案路徑 名稱 檔案型別
			file = new File(filenameTemp);
			// 如果檔案不存在，則建立新的檔案
			if (!file.exists()) {
				file.createNewFile();
				bool = true;
//				System.out.println("success create file: " + filenameTemp);
			}
			// 建立檔案成功後，寫入內容到檔案裡
			writeFileContent(filenameTemp, fileContent, append);
		} catch (Exception ex) {
			throw new Exception(className + " " + funcName + " Error: \n" + ex);
		}
		return bool;
	}

	/**
	 * 向檔案中寫入內容
	 * 
	 * @param filePathName 檔名稱
	 * @param newstr   寫入的內容
	 * @return
	 * @throws IOException
	 */
	private static boolean writeFileContent(String filePathName, String newstr, boolean append) throws Exception {
		String funcName = "writeFileContent";
		Boolean bool = false;
//		String filein = "\r\n" + newstr + "\r\n";// 新寫入的行，換行
		String temp = "";
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		FileOutputStream fos = null;
		PrintWriter pw = null;
		try {
			File file = new File(filePathName);// 檔案路徑(包括檔名稱)
			// 將原檔案內容讀入輸入流
			fis = new FileInputStream(file);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			StringBuffer buffer = new StringBuffer();
			if(append) {
				// 寫入檔案原有內容
				while((temp = br.readLine()) != null) {
					buffer.append(temp);
					// 行與行之間的分隔符 相當於“\n”
					buffer = buffer.append(System.getProperty("line.separator"));
				}
			}
			newstr = append ? "\r\n" + newstr + "\r\n" : newstr;
			buffer.append(newstr);
			fos = new FileOutputStream(file);
			pw = new PrintWriter(fos);
			pw.write(buffer.toString().toCharArray());
			pw.flush();
			bool = true;
		} catch (Exception ex) {
			throw new Exception(className + " " + funcName + " Error: \n" + ex);
		} finally {
			if (pw != null)	pw.close();
			if (fos != null) fos.close();
			if (br != null) br.close();
			if (isr != null) isr.close();
			if (fis != null) fis.close();
		}
		return bool;
	}

	/**
	 * 讀取檔案內容
	 */
	public static String readFileContent(String filePathName) throws Exception {
		String funcName = "readFileContent";
		String temp = "";
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		try {
			File file = new File(filePathName);// 檔案路徑(包括檔名稱)
			// 將檔案內容讀入輸入流
			fis = new FileInputStream(file);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			StringBuffer buffer = new StringBuffer();
			// 讀取檔案內容
			while((temp = br.readLine()) != null) {
				buffer.append(temp);
				// 行與行之間的分隔符 相當於“\n”
				buffer.append(System.getProperty("line.separator"));
			}
			
//			System.out.println(buffer);
			return buffer.toString();
		} catch (Exception ex) {
			throw new Exception(className + " " + funcName + " Error: \n" + ex);
		} finally {
			if (br != null) br.close();
			if (isr != null) isr.close();
			if (fis != null) fis.close();
		}
//		return "";
	}
	
	/**
	 * 刪除路徑下的所有資料夾與資料
	 * @param path
	 */
	public static void deleteFolder(String path) {
		File f = new File(path);
		
		if (!f.exists())
			f.mkdirs();
		
		FileTools.deleteFolder(f);
	}
	/**
	 * 刪除路徑下的所有資料夾與資料
	 * @param file
	 */
	private static void deleteFolder(File file) {
		for (File subFile : file.listFiles()) {
			if (subFile.isDirectory()) {
				deleteFolder(subFile);
			} else {
				subFile.delete();
			}
		}
		file.delete();
	}

	/**
	 * 複製檔案
	 * @param file
	 * @throws Exception 
	 */
	public static void copyFile(String srcDirPath, String destDirPath) throws Exception {
		try {
			FileUtils.copyDirectory(new File(srcDirPath), new File(destDirPath));
		} catch (IOException ex) {
			throw new Exception(className + " copyFile Error: \n" + ex);
		}
	}

}
