# 使用方式:
```sh
	一、將以下四個檔案放在同一目錄下
		1.TableLayout
		2.bat檔
		3.jar檔
		4.config.properties檔
	二、修改config.properties檔裡的參數值
		runType:
			1: Excel無邏輯相關頁籤
			3: 只需將資料提供的文字檔轉成Excel
				(Layout頁籤的"文字檔檔名"為必填，並須將對應的TXT檔放入"POST-ParseExcel2HQL/TableLayout/TXTFile/"目錄下)
			2: 其它情況皆為2
	三、ODS頁籤的"來源文字檔檔名"可不填，若有填則需將對應的TXT檔放入"POST-ParseExcel2HQL/TableLayout/TXTFile/"目錄下

```

# 執行方式:
```sh
	點兩下bat檔即會跳出CMD畫面並自動執行

```

# 產出結果:
```sh
	執行完後會將產出的HQL會對應Excel檔名分別放入同檔名的資料夾下
	1.若Excel無邏輯相關頁籤則程式會自動產出1對1抄的簡易邏輯頁籤與對應HQL
	2.若Excel有邏輯相關頁籤則程式會依頁籤內容產出對應HQL
	3.若有放置來源文字檔則程式會自動將來源文字檔轉成Excel並整理出數值型態欄位的加總值,另會再判斷NotNull欄位是否有Null值，若有則報錯
	4.程式會自動產出驗測時所需SQL(含DW/M與ODS)，但維度分群與資料抽樣的SQL需再自行調整
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