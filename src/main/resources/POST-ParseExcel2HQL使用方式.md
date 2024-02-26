
# 執行方式:
```sh
	點兩下bat檔即會跳出CMD畫面並自動執行

```

# 使用方式:
```sh
	一、先更新SVN:dw2209\DOCUMENT\5-ST\測試紀錄清單.doc
	二、將整理好的layout文件放入TableLayout目錄下
		* "TableLayout\SAMPLE TableLayout"裡面的Excel為範例檔，黃底部份為程式會抓的欄位
		* 一次只能執行同類型的layout
			1.同為DW的收載
			2.同為DW的梳理
			3.同為DM的收載
			4.同為DM的梳理

	三、若為收載則可將來源文字檔放入"TableLayout\TXTFile\"下
		單檔大小不可超過10M(視執行的pc效能而訂)，不然會爆OutOfMemory

	四、修改config.properties檔裡的參數值
		* hadoop.raw.dbname: Hadoop raw的DBNAME
		* hadoop.tmp.dbname: Hadoop tmp的DBNAME
		* hadoop.meta.dbname: Hadoop meta的DBNAME
		* hadoop.std.dbname: Hadoop std的DBNAME
		* mssql.dbname: CREATE MSSQL TABLE所屬DBNAME
			DDWQDWSA: DW
			DDWQDMSA: DM

		* runType:
			1: 產出收載所需程式時使用(需含有ODS頁籤)
			2: 其它情況皆為2
		
		* exportfile: 是否需產出資料提供exportfile檔案
			Y: 是
			N: 否
		
		* exportfile.dbname: 資料提供的資料來源從何而來
			post1_post_poc_tmp: 從邏輯梳理而來
			post1_post_poc_raw: 從AP載入後直接提供出去(例:ACACCOUNT)

		* chksourcefilecontent: 是否檢查來源文字檔的資料內容
			(目前固定為N)
		* svnPath: 本機的svn路徑(特殊符號需加跳脫字元)
			例:C:\\SVN\\dw2209\\

```

# 產出結果:
```sh
	產出的檔案會放在"Output"目錄下
	一、程式上版{執行日}.xlsx: 列出"測試紀錄清單"裡所需資訊(測試者需改成你自己)
	二、PushScript.sql: 列出程式要上至POST環境時所需的Script(取自己需要的即可)
	三、CreateTableScript{執行日}.hql: 列出上至POST環境時所需的Create Table Script
	四、程式相關Sample檔(會放入對應layout的Excel檔名資料夾下)
		1.{來源文字檔檔名}_{Layout的Excel檔名}.xlsx: 
			若有放置來源文字檔則程式會自動將來源文字檔轉成Excel並整理出數值型態欄位的加總值
		2.測試紀錄-{測試紀錄編號}(對應"程式上版{執行日}.xlsx"的編號).doc: 
			Sample的測試紀錄檔(需自行修改)
		3.RCPT.hql: 驗測時所需SQL(需自行修改)
		註:上述三個檔案可自行獨立放置，其餘檔案才是ETL程式所需的Sample Code
```


# <<hql命名規則>>

#####`[資料階段,2~3碼,ODS/DW/DM]_[程式類型,1碼, C/E/T/L][序號,2碼,01開始]{OPTION:_[有意義英數]}`

```sh
	1.[資料階段,2~3碼,ODS/DW/DM]
		ODS：若有ODS頁籤(表示有來源資料轉入ODS資料表的步驟)，則以此規則命名
		DW/DM：擷取主資料表名稱(Layout頁籤E1欄位)第6碼，若為W則轉換為DW；若為M則轉換為DM

	2.[程式類型,1碼,C/E/T/L]
		C-共用
		E-資料擷取：欄位處理邏輯頁籤A欄第一碼為E的步驟
		T-資料轉換：邏輯處理→欄位處理邏輯頁籤A欄第一碼為T的步驟
		L-資料塞入目的表格：欄位處理邏輯頁籤A欄第一碼為L的步驟

	3.[序號,2碼,01開始]
		欄位處理邏輯頁籤之步驟欄位，後2碼

	
	4.{OPTION:_[有意義英數]}
		英數字串限10碼，程式類型為C時，定義為有意義且可辨識之字串

	5.create script的命名:create_[資料表名稱].hql
```


# <<欄位處理邏輯頁籤→目的資料表 命名規則>>

#####`[dbname].[主要資料表名稱]_[10碼,有意義英數]`

```sh
	1.[dbname]:config.properties內設定

	2.[主要資料表名稱]:最終轉入之資料表名稱(從規格之Layout頁籤E1欄位)

	3.[10碼,有意義英數]:以英數字元自定義可辨識的名稱
```