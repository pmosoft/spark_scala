SELECT
       A.TAB_NM
	 , A.TAB_HNM
	 , A.COL_ID
	 , A.COL_NM
	 , A.COL_HNM
	 , A.DATA_TYPE_DESC
	 , A.NULLABLE
	 , A.PK
	 , A.LEN
	 , A.DECIMAL_CNT
	 , A.TAB_ROWS
FROM   TDACM00080 A
WHERE  UPPER(A.TAB_NM ) IN (
 'SCENARIO'
,'SCHEDULE'
,'NRSYSTEM'
,'NRSECTORPARAMETER'
,'MOBILE_PARAMETER'
,'RU_ANTENA'
,'SITE'
,'SECTOR'
,'DU'
,'RU'
,'ANALYSIS_LIST'
)
ORDER BY 1 ASC,A.JDBC_NM,A.OWNER,A.TAB_NM,A.COL_ID

