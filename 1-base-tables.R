meno_base_complete <- "sailw1448v.menopause_base_cohort_complete"
meno_surgical_secondary <- "SAILW1448V.SURGICALMENOPAUSE2_2024"
meno_surgical_primary <- "SAILW1448V.SURGICALMENOPAUSE2024"
meno_early <- "SAILW1448V.EARLYMENOPAUSE2024"
meno_hrt <- "sailw1448v.HRTCOHORT2024"
meno_monitoring <- "SAILW1448V.Monitoring2024"
meno_symptoms <- "SAILW1448V.MENOPAUSESymptoms2024"
meno_age <- "sailw1448v.meno_age"
meno_demographics<- "sailw1448v.meno_demographics"
meno_date<- "sailw1448v.meno_base_cohort"


drop_table(conn,paste("",meno_base_complete ,"",sep=""))
runSQL(conn,paste0("create table ",meno_base_complete,"
(alf_pe bigint,
  first_known_menopause_start_dt date,
  event_cd varchar(1000),
  evidence_type varchar(1000),
  table_index varchar(1000), 
  menopause_type varchar(250)) "
, sep=""))


drop_table(conn,paste("",meno_surgical_secondary ,"",sep=""))
runSQL(conn,paste0("create table ",meno_surgical_secondary," AS 
(SELECT * FROM (
SELECT distinct ALF_PE, OPER_DT AS SURGERY_DT, OPER_CD AS SURGERY_CD,
CASE WHEN OPER_CD LIKE 'Q071%' THEN 'Abdominal hysterocolpectomy and excision of periuterine tissue' WHEN 
OPER_CD LIKE 'Q072%' THEN 'Abdominal hysterectomy and excision of periuterine tissue NEC' WHEN 
OPER_CD LIKE 'Q073%' THEN 'Abdominal hysterocolpectomy NEC' WHEN 
OPER_CD LIKE 'Q074%' THEN  'Total abdominal hysterectomy NEC' WHEN 
OPER_CD LIKE 'Q075%' THEN  'Subtotal abdominal hysterectomy' WHEN 
OPER_CD LIKE 'Q076%' THEN 'Excision of accessory uterus' WHEN 
OPER_CD LIKE 'Q078%' THEN  'Other specified abdominal excision of uterus' WHEN 
OPER_CD LIKE 'Q079%' THEN  'Unspecified abdominal excision of uterus' WHEN 
OPER_CD LIKE 'Q081%' THEN  'Vaginal hysterocolpectomy and excision of periuterine tissue' WHEN 
OPER_CD LIKE 'Q082%' THEN  'Vaginal hysterectomy and excision of periuterine tissue NEC' WHEN 
OPER_CD LIKE 'Q083%' THEN  'Vaginal hysterocolpectomy NEC' WHEN 
OPER_CD LIKE 'Q088%' THEN 'Other specified vaginal excision of uterus' WHEN 
OPER_CD LIKE 'Q089%' THEN 'Unspecified vaginal excision of uterus' WHEN 
OPER_CD LIKE 'Q221%' THEN  'Bilateral salpingoophorectomy' WHEN 
OPER_CD LIKE 'Q222%' THEN 'Bilateral salpingectomy NEC' WHEN 
OPER_CD LIKE 'Q223%' THEN 'Bilateral oophorectomy NEC' WHEN 
OPER_CD LIKE 'Q228%' THEN 'Other specified bilateral excision of adnexa of uterus' WHEN 
OPER_CD LIKE 'Q229%' THEN'Unspecified bilateral excision of adnexa of uterus' WHEN
OPER_CD LIKE 'Q232%' THEN 'Salpingoophorectomy of remaining solitary fallopian tube and ovary' WHEN 
OPER_CD LIKE 'Q231%' THEN 'Unilateral salpingoophorectomy NEC' WHEN 
OPER_CD LIKE 'Q233%' THEN 'Unilateral salpingectomy NEC' WHEN 
OPER_CD LIKE 'Q234%' THEN  'Salpingectomy of remaining solitary fallopian tube NEC' WHEN 
OPER_CD LIKE 'Q235%' THEN  'Unilateral oophorectomy NEC' WHEN 
OPER_CD LIKE 'Q236%' THEN 'Oophorectomy of remaining solitary ovary NEC' WHEN 
OPER_CD LIKE 'Q241%' THEN 'Salpingoophorectomy NEC' WHEN 
OPER_CD LIKE 'Q242%' THEN 'Salpingectomy NEC' WHEN 
OPER_CD LIKE 'Q243%' THEN  'Oophorectomy NEC' WHEN 
OPER_CD LIKE 'Q248%' THEN  'Other specified other excision of adnexa of uterus' WHEN 
OPER_CD LIKE 'Q249%' THEN  'Unspecified other excision of adnexa of uterus' WHEN 
OPER_CD LIKE 'R251%' THEN  'Caesarean hysterectomy'
ELSE 'None' END AS SURGERY
FROM SAIL1448V.PEDW_SINGLE_OPER_20240701
)
WHERE SURGERY NOT LIKE 'None%') WITH DATA; "
                   , sep=""))


runSQL(conn,paste0("INSERT INTO ",meno_base_complete," (alf_pe, FIRST_KNOWN_MENOPAUSE_START_DT, event_cd, evidence_type, table_index, menopause_type)
SELECT ALF_PE, 
surgery_DT AS FIRST_KNOWN_MENOPAUSE_START_DT, 
surgery_cd AS event_cd, 
SURGERY as evidence_type, 
'SURGICALMENOPAUSE2_2024' AS table_index,
'Surgical_menopause' AS menopause_type
FROM SAILW1448V.SURGICALMENOPAUSE2_2024 
WHERE surgery_cd LIKE  'Q071%' OR
surgery_cd LIKE 'Q072%' OR
surgery_cd LIKE  'Q073%' OR 
surgery_cd LIKE  'Q074%' OR 
surgery_cd LIKE  'Q075%' OR
surgery_cd LIKE 'Q078%' OR
surgery_cd LIKE 'Q079%' OR
surgery_cd LIKE 'Q081%' OR
surgery_cd LIKE 'Q082%' OR
surgery_cd LIKE 'Q083%' OR
surgery_cd LIKE 'Q088%' OR
surgery_cd LIKE 'Q089%' OR
surgery_cd LIKE 'Q221%' OR
surgery_cd LIKE 'Q222%' OR
surgery_cd LIKE 'Q223%' OR
surgery_cd LIKE 'Q228%' OR
surgery_cd LIKE 'Q229%' OR
surgery_cd LIKE 'Q232%' OR
surgery_cd LIKE 'Q234%' OR
surgery_cd LIKE 'Q236%' OR
surgery_cd LIKE 'Q241%' OR
surgery_cd LIKE 'Q243%' OR
surgery_cd LIKE 'Q248%' OR
surgery_cd LIKE'Q249%' ; "
                   , sep=""))



drop_table(conn,paste("",meno_surgical_primary ,"",sep=""))
runSQL(conn,paste0("create table ",meno_surgical_primary," AS  (SELECT * FROM (
SELECT distinct ALF_PE, EVENT_DT as SurgicalMeno_DT, EVENT_CD as SurgicalMeno_CD,
CASE WHEN
EVENT_CD LIKE '1599%' THEN 'h/o: hysterectomy' when
EVENT_CD LIKE '685H%' THEN 'no smear - benign hysterectomy' WHEN
EVENT_CD LIKE '7E04.%' THEN 'Abdominal excision of uterus' WHEN
EVENT_CD LIKE '7E041%' THEN 'Abdominal hysterectomy & excision of periuterine tissue NEC' WHEN
EVENT_CD LIKE '7E042%' THEN 'Abdominal hysterocolpectomy NEC' WHEN
EVENT_CD LIKE '7E043%' THEN 'Total abdominal hysterectomy NEC' WHEN
EVENT_CD LIKE '7E044%' THEN 'Subtotal abdominal hysterectomy' WHEN
EVENT_CD LIKE '7E045%' THEN 'Abdominal hysterectomy and bilateral salpingoophorectomy' WHEN
EVENT_CD LIKE '7E046%' THEN 'Radical hysterectomy' WHEN
EVENT_CD LIKE '7E047%' THEN 'Abdominal hysterectomy and right salpingoopherectomy' WHEN 
EVENT_CD LIKE '7E048%' THEN 'Abdominal hysterectomy and left salpingoophorectomy' WHEN
EVENT_CD LIKE '7E049%' THEN 'TAH - Tot abdom hysterectomy and BSO - bilat salpingophorect' WHEN 
EVENT_CD LIKE '7E04A%' THEN 'Abdominal hysterectomy with conservation of ovaries' WHEN
EVENT_CD LIKE '7E04B%' THEN 'Lapar total abdominal hysterect bilat salpingo-oophorectomy' WHEN
EVENT_CD LIKE '7E04C%' THEN 'Laparoscopic hysterectomy' WHEN
EVENT_CD LIKE '7E04D%' THEN 'Excision of accessory uterus' WHEN
EVENT_CD LIKE '7E04E%' THEN 'Laparoscopic subtotal hysterectomy' WHEN 
EVENT_CD LIKE '7E04F%' THEN 'Subtotal abdominal hysterectomy with conservation of ovaries' WHEN 
EVENT_CD LIKE '7E04G%' THEN 'Total abdominal hysterectomy with conservation of ovaries' when
EVENT_CD LIKE '7E04H%' THEN 'subtotl abdominal hysterectomy & bilat salpingo-oophorectomy' when
EVENT_CD LIKE '7E04J%' THEN 'subtotl abdominal hysterectomy & right salpingo-oophorectomy' WHEN 
EVENT_CD LIKE '7E04K%' THEN 'subtotal abdominal hysterectomy & left salpingo-oophorectomy' WHEN 
EVENT_CD LIKE '7E04N%' THEN 'radical hysterectomy with conservation of ovaries' when
EVENT_CD LIKE '7E04P%' THEN 'radical hysterectomy with bilateral salpingo-oophorectomy' when
EVENT_CD LIKE '7E04y%' THEN 'Other specified abdominal excision of uterus' WHEN 
EVENT_CD LIKE '7E04z%' THEN 'Abdominal excision of uterus NOS' WHEN 
EVENT_CD LIKE '7E055%' THEN 'Vaginal hysterectomy with conservation of ovaries' WHEN 
EVENT_CD LIKE '7E052%' THEN 'Vaginal hysterocolpectomy NEC' when
EVENT_CD LIKE '7E053%' THEN 'Vaginal hysterectomy NEC' WHEN 
EVENT_CD LIKE '7E054%' THEN 'Laparoscopic vaginal hysterectomy' WHEN 
EVENT_CD LIKE '7E056%' THEN 'Lap assist vag hysterectomy with bilat salpingo-oophorectomy' wHEN 
EVENT_CD LIKE '7E057%' THEN 'vaginal hysterectomy and right salpingo-oophorectomy' wHEN 
EVENT_CD LIKE '7E058%' THEN 'vaginal hysterectomy and left salpingo-oophorectomy' when
EVENT_CD LIKE '7E05y%' THEN 'Other specified vaginal excision of uterus' WHEN 
EVENT_CD LIKE '7E05z%' THEN 'Vaginal excision of uterus NOS' WHEN 
EVENT_CD LIKE '7F1A0%' THEN 'Caesarean hysterectomy' WHEN 
EVENT_CD LIKE '8L70%' THEN 'Hysterectomy planned' WHEN 
EVENT_CD LIKE '9O8W%' THEN 'Cervical smear to continue post hysterectomy' WHEN 
EVENT_CD LIKE 'K515%' THEN 'Post hysterectomy vaginal vault prolapse' WHEN 
EVENT_CD LIKE 'L3985%' THEN 'Delivery by caesarean hysterectomy' ELSE 'None' END AS SurgicalMenopause
FROM SAIL1448V.WLGP_GP_EVENT_CLEANSED_20240401
WHERE GNDR_CD=2)
WHERE SurgicalMenopause NOT LIKE 'None%') WITH DATA; "
       , sep=""))


runSQL(conn,paste0("INSERT INTO ",meno_base_complete," 
(alf_pe, FIRST_KNOWN_MENOPAUSE_START_DT, event_cd, evidence_type, table_index, menopause_type)
SELECT ALF_PE, 
SurgicalMeno_DT AS FIRST_KNOWN_MENOPAUSE_START_DT, 
SurgicalMeno_CD AS event_cd, 
SurgicalMenopause as evidence_type, 
'SURGICALMENOPAUSE2024' AS table_index, 
'SurgicalMenopause' AS menopause_type
FROM SAILW1448V.SURGICALMENOPAUSE2024 
WHERE 
surgicalmeno_cd LIKE '1599%' or
surgicalmeno_cd LIKE '685H%' or
surgicalmeno_cd LIKE '7E04.%' or
surgicalmeno_cd LIKE '7E041%' or
surgicalmeno_cd LIKE '7E042%' or
surgicalmeno_cd LIKE '7E043%' or
surgicalmeno_cd LIKE '7E044%' or
surgicalmeno_cd LIKE '7E045%' or
surgicalmeno_cd LIKE '7E046%' or
surgicalmeno_cd LIKE '7E049%' or
surgicalmeno_cd LIKE '7E04B%' or
surgicalmeno_cd LIKE '7E04C%' or
surgicalmeno_cd LIKE '7E04D%' or
surgicalmeno_cd LIKE '7E04E%' or
surgicalmeno_cd LIKE '7E04H%' or
surgicalmeno_cd LIKE '7E04P%' or
surgicalmeno_cd LIKE '7E04y%' or
surgicalmeno_cd LIKE '7E04z%' or
surgicalmeno_cd LIKE '7E052%' or
surgicalmeno_cd LIKE '7E053%' or
surgicalmeno_cd LIKE '7E054%' or
surgicalmeno_cd LIKE '7E056%' or
surgicalmeno_cd LIKE '7E05y%' or
surgicalmeno_cd LIKE '7E05z%' or
surgicalmeno_cd LIKE 'K515%';"
                   , sep=""))



drop_table(conn,paste("",meno_early ,"",sep=""))
runSQL(conn,paste0("create table ",meno_early," AS (SELECT * FROM (
SELECT distinct ALF_PE, EVENT_DT as EarlyMeno_DT, EVENT_CD as EarlyMeno_CD,
CASE WHEN EVENT_CD	LIKE 'C162.%' THEN 'Postablative ovarian failure' WHEN 
EVENT_CD	LIKE 	'C1620%'	THEN 'Postsurgical ovarian failure' when
EVENT_CD	LIKE 	'C1621%'	THEN 'Postirradiation ovarian failure' WHEN
EVENT_CD	LIKE 	'C1622%' THEN 'Other iatrogenic postablative ovarian failure' when
EVENT_CD	LIKE 	'C162y%'	THEN 'Other specified postablative ovarian failure' when 
EVENT_CD	LIKE 	'C162z%'	THEN 'Postablative ovarian failure NOS' WHEN
EVENT_CD LIKE 'C163.%' THEN 'Other ovarian failure' WHEN 
EVENT_CD	LIKE 	'C1630%'	THEN 'Primary ovarian failure' when
EVENT_CD	LIKE 	'C1631%'	THEN 'Secondary ovarian failure' WHEN
EVENT_CD	LIKE 	'C1632%'	THEN 'Hypergonadotrophic ovarian failure' WHEN
EVENT_CD	LIKE 	'C1633%'	THEN 'Ovarian hypogonadism' WHEN
EVENT_CD	LIKE 	'C1634%'	THEN 'Early menopause' WHEN
EVENT_CD	LIKE 	'C163y%'	THEN 'Other specified other ovarian failure' when
EVENT_CD	LIKE 	'C163z%'	THEN 'Other ovarian failure NOS' when    
EVENT_CD	LIKE 	'K5A4%'	THEN 'Artificial menopause state' 
 ELSE 'None' END as EarlyMenopause
FROM SAIL1448V.WLGP_GP_EVENT_CLEANSED_20240401
WHERE GNDR_CD=2)
WHERE EarlyMenopause NOT LIKE 'None%') WITH DATA;"
                   , sep=""))


runSQL(conn,paste0("INSERT INTO ",meno_base_complete," 
 (alf_pe, FIRST_KNOWN_MENOPAUSE_START_DT, event_cd, evidence_type, table_index, menopause_type)
SELECT ALF_PE, 
EARLYMENO_DT AS FIRST_known_menopause_start_dt, 
EARLYMENO_CD AS EVENT_CD, 
EarlyMenopause AS evidence_type,
'EARLYMENOPAUSE2024' AS table_index, 
'Early_menopause' AS menopause_type
from SAILW1448V.EARLYMENOPAUSE2024 ; "
                   , sep=""))


drop_table(conn,paste("",meno_hrt ,"",sep=""))
runSQL(conn,paste0("create table ",meno_hrt," AS
(SELECT FILT.ALF_PE, EVENT_DT, EVENT_CD, WOB, HRTTYPE, TOWNSEND_2011_QUINTILE_DESC, WIMD_2014_DECILE_DESC, WIMD_2019_DECILE_DESC, START_DATE, END_DATE
FROM (SELECT ALF_PE, EVENT_CD, EVENT_DT, WOB,
CASE WHEN EVENT_CD LIKE 'ff2W%' OR EVENT_CD LIKE 'ff9K%'OR EVENT_CD LIKE 'ff2O%' OR EVENT_CD LIKE 'ff94%' OR EVENT_CD LIKE 'ff9J%' OR EVENT_CD LIKE 'ff97%'
OR EVENT_CD LIKE 'ff29%' OR EVENT_CD LIKE 'ff2i%' OR EVENT_CD LIKE 'ff2I%' OR EVENT_CD LIKE 'ff2J%' OR EVENT_CD LIKE 'ff9A%' OR EVENT_CD LIKE 'ff2r%'
OR EVENT_CD LIKE 'ff2s%' OR EVENT_CD LIKE 'ff2p%' OR EVENT_CD LIKE 'ff2q%' OR EVENT_CD LIKE 'ff2o%' OR EVENT_CD LIKE 'ff2n%' OR EVENT_CD LIKE 'ff93%'
OR EVENT_CD LIKE 'ff99%' OR EVENT_CD LIKE 'ff9A%' OR EVENT_CD LIKE 'ff9B%' OR EVENT_CD LIKE 'ff9G%' OR EVENT_CD LIKE 'ff9H%' OR EVENT_CD LIKE 'ff9I%'
OR EVENT_CD LIKE 'ff2A%' OR EVENT_CD LIKE 'ff2E%' OR EVENT_CD LIKE 'ff2F%' OR EVENT_CD LIKE 'ff2G%' OR EVENT_CD LIKE 'ff2H%' OR EVENT_CD LIKE 'ff2K%'
OR EVENT_CD LIKE 'ff2P%' OR EVENT_CD LIKE 'ff2Q%'OR EVENT_CD LIKE 'ff2R%' OR EVENT_CD LIKE 'ff2S%' OR EVENT_CD LIKE 'ff2U%' OR EVENT_CD LIKE 'ff2V%'
OR EVENT_CD LIKE 'ff2W%'OR EVENT_CD LIKE 'ff2a%' OR EVENT_CD LIKE 'ff2b%' OR EVENT_CD LIKE 'ff2c%' OR EVENT_CD LIKE 'ff2j%' OR EVENT_CD LIKE 'ff2k%'
OR EVENT_CD LIKE 'ff2j%' OR EVENT_CD LIKE 'ff92%' THEN 'Oestrogen Patches' 
WHEN EVENT_CD LIKE 'ff2C%' OR EVENT_CD LIKE 'ff91%'OR EVENT_CD LIKE 'ff2D%' OR EVENT_CD LIKE 'ff2X%' OR EVENT_CD LIKE 'ff2Y%' OR EVENT_CD LIKE 'ff95%'
OR EVENT_CD LIKE 'ff96%' OR EVENT_CD LIKE 'ff2Z%' THEN 'Oestrogen gels'
WHEN
EVENT_CD LIKE 'ff9c%' OR EVENT_CD LIKE 'ff9z%' THEN 'Oestrogen nasal sprays'
WHEN 
EVENT_CD LIKE 'ff21%' OR EVENT_CD LIKE 'ff2v%'OR EVENT_CD LIKE 'ff23%' 
OR EVENT_CD LIKE 'ff2u%' OR EVENT_CD LIKE 'ff22%' OR EVENT_CD LIKE 'ff2t%'
OR EVENT_CD LIKE 'ff96%' OR EVENT_CD LIKE 'ff2Z%' OR EVENT_CD LIKE 'x01M4%' THEN 'Oestrogen implants'
WHEN  
EVENT_CD LIKE 'ff25%' OR EVENT_CD LIKE 'ff2w%' OR EVENT_CD LIKE 'ff24%' 
OR EVENT_CD LIKE 'ff2x%' THEN 'Oestrogen injections'
WHEN 
EVENT_CD LIKE 'ff9D%' OR EVENT_CD LIKE 'ff9y%' OR EVENT_CD LIKE 'ff9M%' OR EVENT_CD LIKE 'ff2d%'
OR EVENT_CD LIKE 'x025v%' OR EVENT_CD LIKE 'x025s%'
OR EVENT_CD LIKE 'g352%' OR EVENT_CD LIKE 'g351%'
OR EVENT_CD LIKE 'g331%' OR EVENT_CD LIKE 'g334%' THEN 'Oestrogen vaginal pessaries/tablets'
WHEN 
EVENT_CD LIKE 'ff14%' OR EVENT_CD LIKE 'ff81%'OR EVENT_CD LIKE 'ff2y%' OR EVENT_CD LIKE 'ff28%' 
OR EVENT_CD LIKE 'ff44%' OR EVENT_CD LIKE 'ff2z%'OR EVENT_CD LIKE 'ff33%' OR EVENT_CD LIKE 'ff9E%' 
OR EVENT_CD LIKE 'ff26%' OR EVENT_CD LIKE 'ff2T%'OR EVENT_CD LIKE 'ff3z%' OR EVENT_CD LIKE 'ff61%' 
OR EVENT_CD LIKE 'ff13%' OR EVENT_CD LIKE 'ff2g%'OR EVENT_CD LIKE 'ff11%' OR EVENT_CD LIKE 'ff98%' 
OR EVENT_CD LIKE 'ff5z%' OR EVENT_CD LIKE 'ff4w%'OR EVENT_CD LIKE 'ff51%' OR EVENT_CD LIKE 'ff12%' 
OR EVENT_CD LIKE 'ff4y%' OR EVENT_CD LIKE 'ff41%'OR EVENT_CD LIKE 'ff4x%' OR EVENT_CD LIKE 'ff27%' 
OR EVENT_CD LIKE 'ff7z%' OR EVENT_CD LIKE 'ff71%'OR EVENT_CD LIKE 'ff9L%' OR EVENT_CD LIKE 'ff42%' 
OR EVENT_CD LIKE 'ff9F%' OR EVENT_CD LIKE 'ff31%'OR EVENT_CD LIKE 'ff2N%' OR EVENT_CD LIKE 'ff43%' 
OR EVENT_CD LIKE 'ff2m%' OR EVENT_CD LIKE 'ff6y%'OR EVENT_CD LIKE 'ff83%' OR EVENT_CD LIKE 'ff32%' 
OR EVENT_CD LIKE 'ff4z%' OR EVENT_CD LIKE 'ff2f%'OR EVENT_CD LIKE 'ff2h%' OR EVENT_CD LIKE 'ff82%'
OR EVENT_CD LIKE 'ff2L%' OR EVENT_CD LIKE 'ff2M%' OR EVENT_CD LIKE 'x01M3%' THEN 'Oestrogen tablets'
WHEN 
EVENT_CD LIKE 'x00gH%' OR EVENT_CD LIKE'x025u%' OR EVENT_CD LIKE'x025w%' OR EVENT_CD LIKE 'x025t%' 
OR EVENT_CD LIKE 'x00G5%' OR EVENT_CD LIKE 'x00HB%' OR EVENT_CD LIKE 'g333%'
OR EVENT_CD LIKE 'g335%' OR EVENT_CD LIKE 'g332%' OR EVENT_CD LIKE 'g33z%'
OR EVENT_CD LIKE 'g336%' OR EVENT_CD LIKE 'g33y%' OR EVENT_CD LIKE 'g311%'
OR EVENT_CD LIKE 'g312%' OR EVENT_CD LIKE 'g31.%' OR EVENT_CD LIKE 'g32.%'
OR EVENT_CD LIKE 'g322%' OR EVENT_CD LIKE 'g32z%' OR EVENT_CD LIKE 'g321%' THEN 'Oestrogen vaginal cream'
WHEN 
EVENT_CD LIKE 'fh1h%' OR EVENT_CD LIKE 'fh14%' OR EVENT_CD LIKE 'fh1F%' OR EVENT_CD LIKE 'fh1E%' OR EVENT_CD LIKE 'fh1L%'  
OR EVENT_CD LIKE 'fh17%' OR EVENT_CD LIKE 'fh1R%' OR EVENT_CD LIKE 'fh11%' OR EVENT_CD LIKE 'fh1x%' OR EVENT_CD LIKE 'fh1u%'  
OR EVENT_CD LIKE 'fh1b%' OR EVENT_CD LIKE 'fh1S%' OR EVENT_CD LIKE 'fh16%' OR EVENT_CD LIKE 'fh21%' OR EVENT_CD LIKE 'fh1g%'  
OR EVENT_CD LIKE 'fh1V%' OR EVENT_CD LIKE 'fh1X%' OR EVENT_CD LIKE 'fh1W%' OR EVENT_CD LIKE 'fh1z%' OR EVENT_CD LIKE 'fh1U%'  
OR EVENT_CD LIKE 'fh1C%' OR EVENT_CD LIKE 'fh1D%' OR EVENT_CD LIKE 'fh1B%' OR EVENT_CD LIKE 'fh1y%' OR EVENT_CD LIKE 'fh1J%' 
OR EVENT_CD LIKE 'fh1K%' OR EVENT_CD LIKE 'fh1s%' OR EVENT_CD LIKE 'fh19%' OR EVENT_CD LIKE 'fh1Q%' OR EVENT_CD LIKE 'fh12%'  
OR EVENT_CD LIKE 'fh1l%' OR EVENT_CD LIKE 'fh13%' OR EVENT_CD LIKE 'fh1d%' OR EVENT_CD LIKE 'fh1f%' OR EVENT_CD LIKE 'fh1v%'  
OR EVENT_CD LIKE 'fh1M%' OR EVENT_CD LIKE 'fh1T%' OR EVENT_CD LIKE 'fh1G%' OR EVENT_CD LIKE 'fh1j%' OR EVENT_CD LIKE 'fh1H%' 
OR EVENT_CD LIKE 'fh15%' OR EVENT_CD LIKE 'fh1w%' OR EVENT_CD LIKE 'fh18%' OR EVENT_CD LIKE 'fh1k%' OR EVENT_CD LIKE 'fh1t%'
OR EVENT_CD LIKE 'fh1a%' OR EVENT_CD LIKE 'fh1c%' OR EVENT_CD LIKE 'fh1o%'
OR EVENT_CD LIKE 'x00fr%' OR EVENT_CD LIKE 'x00fq%' OR EVENT_CD LIKE 'x02cL5%'
OR EVENT_CD LIKE 'x00fW%' THEN 'Combined tablets'
WHEN 
EVENT_CD LIKE 'fh1Y%' OR EVENT_CD LIKE 'fh1e%' OR EVENT_CD LIKE 'fh1O%' OR EVENT_CD LIKE 'fh1m%' 
OR EVENT_CD LIKE 'fh1N%' OR EVENT_CD LIKE 'fh1q%' OR EVENT_CD LIKE 'fh1P%' OR EVENT_CD LIKE 'fh1p%' OR EVENT_CD LIKE 'fh1A%'  
OR EVENT_CD LIKE 'fh1n%'
OR EVENT_CD LIKE 'x03v8%' OR EVENT_CD LIKE 'x04bM%' OR EVENT_CD LIKE 'x056u%' OR EVENT_CD LIKE 'x04bL%'
OR EVENT_CD LIKE 'x05iJ%' OR EVENT_CD LIKE 'x025q%' OR EVENT_CD LIKE 'x05fB%' 
THEN 'Combined patches'
WHEN 
EVENT_CD LIKE 'fi4G%' OR EVENT_CD LIKE 'fi4w%' OR
EVENT_CD LIKE 'fi42%' OR EVENT_CD LIKE 'fi41%' OR
EVENT_CD LIKE 'fi4H%' OR EVENT_CD LIKE 'fi4C%' OR EVENT_CD LIKE 'fi4F%' 
OR EVENT_CD LIKE 'fi4v%' OR EVENT_CD LIKE 'fi4B%' THEN 'Testosterone gels, implants, or patches'
ELSE 'None' END AS HRTtype
FROM SAIL1448V.WLGP_GP_EVENT_CLEANSED_20240401) HRTcoding
inner JOIN 
(SELECT distinct ALF_PE FROM SAIL1448V.WLGP_GP_EVENT_CLEANSED_20240401 wgec 
WHERE EVENT_CD LIKE 'fi4H%' OR EVENT_CD LIKE 'ff2W%' OR EVENT_CD LIKE 'ff9K%'OR EVENT_CD LIKE 'ff2O%' OR EVENT_CD LIKE 'ff94%' OR EVENT_CD LIKE 'ff9J%' OR EVENT_CD LIKE 'ff97%'
OR EVENT_CD LIKE 'ff29%' OR EVENT_CD LIKE 'ff2i%' OR EVENT_CD LIKE 'ff2I%' OR EVENT_CD LIKE 'ff2J%' OR EVENT_CD LIKE 'ff9A%' OR EVENT_CD LIKE 'ff2r%'
OR EVENT_CD LIKE 'ff2s%' OR EVENT_CD LIKE 'ff2p%' OR EVENT_CD LIKE 'ff2q%' OR EVENT_CD LIKE 'ff2o%' OR EVENT_CD LIKE 'ff2n%' OR EVENT_CD LIKE 'ff93%'
OR EVENT_CD LIKE 'ff99%' OR EVENT_CD LIKE 'ff9A%' OR EVENT_CD LIKE 'ff9B%' OR EVENT_CD LIKE 'ff9G%' OR EVENT_CD LIKE 'ff9H%' OR EVENT_CD LIKE 'ff9I%'
OR EVENT_CD LIKE 'ff2A%' OR EVENT_CD LIKE 'ff2E%' OR EVENT_CD LIKE 'ff2F%' OR EVENT_CD LIKE 'ff2G%' OR EVENT_CD LIKE 'ff2H%' OR EVENT_CD LIKE 'ff2K%'
OR EVENT_CD LIKE 'ff2P%' OR EVENT_CD LIKE 'ff2Q%'OR EVENT_CD LIKE 'ff2R%' OR EVENT_CD LIKE 'ff2S%' OR EVENT_CD LIKE 'ff2U%' OR EVENT_CD LIKE 'ff2V%'
OR EVENT_CD LIKE 'ff2W%'OR EVENT_CD LIKE 'ff2a%' OR EVENT_CD LIKE 'ff2b%' OR EVENT_CD LIKE 'ff2c%' OR EVENT_CD LIKE 'ff2j%' OR EVENT_CD LIKE 'ff2k%'
OR EVENT_CD LIKE 'ff2j%' OR EVENT_CD LIKE 'ff92%' OR EVENT_CD LIKE 'ff2C%' OR EVENT_CD LIKE 'ff91%'OR EVENT_CD LIKE 'ff2D%' OR EVENT_CD LIKE 'ff2X%' 
OR EVENT_CD LIKE 'ff2Y%' OR EVENT_CD LIKE 'ff95%'
OR EVENT_CD LIKE 'ff96%' OR EVENT_CD LIKE 'ff2Z%' OR
EVENT_CD LIKE 'ff9c%' OR EVENT_CD LIKE 'ff9z%' OR 
EVENT_CD LIKE 'ff21%' OR EVENT_CD LIKE 'ff2v%'OR EVENT_CD LIKE 'ff23%' OR EVENT_CD LIKE 'ff2u%' OR EVENT_CD LIKE 'ff22%' OR EVENT_CD LIKE 'ff2t%'
OR EVENT_CD LIKE 'ff96%' OR EVENT_CD LIKE 'ff2Z%' OR
EVENT_CD LIKE 'ff25%' OR EVENT_CD LIKE 'ff2w%'OR EVENT_CD LIKE 'ff24%' OR EVENT_CD LIKE 'ff2x%' OR
EVENT_CD LIKE 'ff9D%' OR EVENT_CD LIKE 'ff9y%'OR EVENT_CD LIKE 'ff9M%' OR EVENT_CD LIKE 'ff2d%' OR
EVENT_CD LIKE 'ff14%' OR EVENT_CD LIKE 'ff81%'OR EVENT_CD LIKE 'ff2y%' OR EVENT_CD LIKE 'ff28%' 
OR EVENT_CD LIKE 'ff44%' OR EVENT_CD LIKE 'ff2z%'OR EVENT_CD LIKE 'ff33%' OR EVENT_CD LIKE 'ff9E%' 
OR EVENT_CD LIKE 'ff26%' OR EVENT_CD LIKE 'ff2T%'OR EVENT_CD LIKE 'ff3z%' OR EVENT_CD LIKE 'ff61%' 
OR EVENT_CD LIKE 'ff13%' OR EVENT_CD LIKE 'ff2g%'OR EVENT_CD LIKE 'ff11%' OR EVENT_CD LIKE 'ff98%' 
OR EVENT_CD LIKE 'ff5z%' OR EVENT_CD LIKE 'ff4w%'OR EVENT_CD LIKE 'ff51%' OR EVENT_CD LIKE 'ff12%' 
OR EVENT_CD LIKE 'ff4y%' OR EVENT_CD LIKE 'ff41%'OR EVENT_CD LIKE 'ff4x%' OR EVENT_CD LIKE 'ff27%' 
OR EVENT_CD LIKE 'ff7z%' OR EVENT_CD LIKE 'ff71%'OR EVENT_CD LIKE 'ff9L%' OR EVENT_CD LIKE 'ff42%' 
OR EVENT_CD LIKE 'ff9F%' OR EVENT_CD LIKE 'ff31%'OR EVENT_CD LIKE 'ff2N%' OR EVENT_CD LIKE 'ff43%' 
OR EVENT_CD LIKE 'ff2m%' OR EVENT_CD LIKE 'ff6y%'OR EVENT_CD LIKE 'ff83%' OR EVENT_CD LIKE 'ff32%' 
OR EVENT_CD LIKE 'ff4z%' OR EVENT_CD LIKE 'ff2f%'OR EVENT_CD LIKE 'ff2h%' OR EVENT_CD LIKE 'ff82%'
OR EVENT_CD LIKE 'ff2L%' OR EVENT_CD LIKE 'ff2M%' OR
EVENT_CD LIKE 'fh1h%' OR EVENT_CD LIKE 'fh14%' OR EVENT_CD LIKE 'fh1F%' OR EVENT_CD LIKE 'fh1E%' OR EVENT_CD LIKE 'fh1L%'  
OR EVENT_CD LIKE 'fh17%' OR EVENT_CD LIKE 'fh1R%' OR EVENT_CD LIKE 'fh11%' OR EVENT_CD LIKE 'fh1x%' OR EVENT_CD LIKE 'fh1u%'  
OR EVENT_CD LIKE 'fh1b%' OR EVENT_CD LIKE 'fh1S%' OR EVENT_CD LIKE 'fh16%' OR EVENT_CD LIKE 'fh21%' OR EVENT_CD LIKE 'fh1g%'  
OR EVENT_CD LIKE 'fh1V%' OR EVENT_CD LIKE 'fh1X%' OR EVENT_CD LIKE 'fh1W%' OR EVENT_CD LIKE 'fh1z%' OR EVENT_CD LIKE 'fh1U%'  
OR EVENT_CD LIKE 'fh1C%' OR EVENT_CD LIKE 'fh1D%' OR EVENT_CD LIKE 'fh1B%' OR EVENT_CD LIKE 'fh1y%' OR EVENT_CD LIKE 'fh1J%' 
OR EVENT_CD LIKE 'fh1K%' OR EVENT_CD LIKE 'fh1s%' OR EVENT_CD LIKE 'fh19%' OR EVENT_CD LIKE 'fh1Q%' OR EVENT_CD LIKE 'fh12%'  
OR EVENT_CD LIKE 'fh1l%' OR EVENT_CD LIKE 'fh13%' OR EVENT_CD LIKE 'fh1d%' OR EVENT_CD LIKE 'fh1f%' OR EVENT_CD LIKE 'fh1v%'  
OR EVENT_CD LIKE 'fh1M%' OR EVENT_CD LIKE 'fh1T%' OR EVENT_CD LIKE 'fh1G%' OR EVENT_CD LIKE 'fh1j%' OR EVENT_CD LIKE 'fh1H%' 
OR EVENT_CD LIKE 'fh15%' OR EVENT_CD LIKE 'fh1w%' OR EVENT_CD LIKE 'fh18%' OR EVENT_CD LIKE 'fh1k%' OR EVENT_CD LIKE 'fh1t%'
OR EVENT_CD LIKE 'fh1a%' OR EVENT_CD LIKE 'fh1c%' OR EVENT_CD LIKE 'fh1o%' OR 
EVENT_CD LIKE 'fh1Y%' OR EVENT_CD LIKE 'fh1e%' OR EVENT_CD LIKE 'fh1O%' OR EVENT_CD LIKE 'fh1m%' 
OR EVENT_CD LIKE 'fh1N%' OR EVENT_CD LIKE 'fh1q%' OR EVENT_CD LIKE 'fh1P%' OR EVENT_CD LIKE 'fh1p%' OR EVENT_CD LIKE 'fh1A%'  
OR EVENT_CD LIKE 'fh1n%' 
OR
EVENT_CD LIKE 'fi4G%' OR EVENT_CD LIKE 'fi4w%' 
OR
EVENT_CD LIKE 'fi42%' OR EVENT_CD LIKE 'fi41%' OR
EVENT_CD LIKE 'fi4H%' OR EVENT_CD LIKE 'fi4C%' OR EVENT_CD LIKE 'fi4F%' OR EVENT_CD LIKE 'fi4v%' OR EVENT_CD LIKE 'fi4B%'
OR 
EVENT_CD LIKE 'x01M3%' OR EVENT_CD LIKE
'x00fr%' OR EVENT_CD LIKE 'x00fq%' OR EVENT_CD LIKE 'x02cL%' OR EVENT_CD LIKE 'x00fW%' OR EVENT_CD LIKE
'ff9y%' OR EVENT_CD LIKE 'ff9D%' OR EVENT_CD LIKE 'g352%' OR EVENT_CD LIKE 'g351%' OR EVENT_CD LIKE
'x025v%' OR EVENT_CD LIKE 'x025s%' OR EVENT_CD LIKE 'g331%' OR EVENT_CD LIKE 'g334%' OR EVENT_CD LIKE
'x01M5%' OR EVENT_CD LIKE 'x00gH%' OR EVENT_CD LIKE 'x025u%' OR EVENT_CD LIKE 'g333%' OR EVENT_CD LIKE
'g335%' OR EVENT_CD LIKE 'x025w%' OR EVENT_CD LIKE 'x025t%' OR EVENT_CD LIKE 'g332%' OR EVENT_CD LIKE
'g33z%' OR EVENT_CD LIKE 'g336%' OR EVENT_CD LIKE 'g33y%' OR EVENT_CD LIKE 'g311%' OR EVENT_CD LIKE
'g312%' OR EVENT_CD LIKE 'g31.%' OR EVENT_CD LIKE 'g32.%' OR EVENT_CD LIKE 'x00G5%' OR EVENT_CD LIKE
'x00HB%' OR EVENT_CD LIKE 'g322%' OR EVENT_CD LIKE 'g32z%' OR EVENT_CD LIKE 'g321%' OR EVENT_CD LIKE
'x03v8%' OR EVENT_CD LIKE 'x04bM%' OR EVENT_CD LIKE 'x056u%' OR EVENT_CD LIKE 'x04bL%' OR EVENT_CD LIKE
'x05iJ%' OR EVENT_CD LIKE 'x025q%' OR EVENT_CD LIKE 'x05fB%' OR EVENT_CD LIKE 'x01M4%'
AND GNDR_CD=2) FILT
ON HRTcoding.ALF_PE =FILT.ALF_PE
INNER JOIN 
(SELECT ALF_PE, START_DATE, END_DATE, TOWNSEND_2011_QUINTILE, TOWNSEND_2011_QUINTILE_DESC, WIMD_2014_DECILE_DESC, WIMD_2019_DECILE_DESC
FROM SAIL1448V.WDSD_SINGLE_CLEAN_GEO_CHAR_LSOA2011_20240701
) DEPR
ON FILT.ALF_PE = DEPR.ALF_PE
WHERE HRTtype NOT LIKE 'None%') WITH DATA;
"
                   , sep=""))


runSQL(conn,paste0("INSERT INTO ",meno_base_complete," 
(alf_pe, FIRST_KNOWN_MENOPAUSE_START_DT, event_cd, evidence_type, table_index, menopause_type)
SELECT ALF_PE, 
EVENT_DT AS FIRST_KNOWN_MENOPAUSE_START_DT, 
EVENT_CD AS EVENT_CD,
HRTTYPE AS evidence_type,
'HRTCOHORT2024' AS table_index,
'Symptomatic_menopause' AS menopause_type
FROM sailw1448v.HRTCOHORT2024 
WHERE HRTTYPE LIKE '%Patches%' OR
HRTTYPE LIKE '%patches%' OR
HRTTYPE LIKE '%tablets%' OR 
HRTTYPE LIKE '%gels%' OR
HRTTYPE LIKE '%injections%' OR
HRTTYPE LIKE '%sprays%' OR
HRTTYPE LIKE '%implants%';"
                   , sep=""))



drop_table(conn,paste("",meno_monitoring ,"",sep=""))
runSQL(conn,paste0("create table ",meno_monitoring," as (SELECT * FROM 
( SELECT distinct ALF_PE, GNDR_CD, EVENT_DT AS Monitor_DT, EVENT_CD AS Monitor_CD,
CASE WHEN EVENT_CD LIKE	'66UK%' THEN 'Hormone replacement therapy bleed pattern - no bleeding' WHEN
EVENT_CD LIKE '66U..' THEN 'Menopause monitoring' WHEN
 EVENT_CD LIKE	'66U7%' THEN 'HRT started'	when
 EVENT_CD LIKE	'66UL%'	THEN 'Years on hormone replacement therapy' when
 EVENT_CD LIKE	'66UI%'	THEN 'Hormone replacement therapy bleed pattern - abnormal' when
 EVENT_CD LIKE	'66U8%'	THEN 'HRT side-effects' when
 EVENT_CD LIKE	'66UB%'	THEN 'HRT: unopposed oestrogen' when
EVENT_CD LIKE	'66UA%' THEN 'HRT stopped' WHEN 
EVENT_CD LIKE	'66UJ%'	THEN 'Hormone replacement therapy bleed pattern - not relevant' when
EVENT_CD LIKE	'66UC%'	THEN 'HRT: combined oestrog/progest' when
EVENT_CD LIKE	'66UH%'	THEN 'Hormone replacement therapy bleed pattern - normal' WHEN 
EVENT_CD LIKE	'66U3%'	THEN 'Menopause symptoms present' WHEN 
EVENT_CD LIKE '66U1%' THEN 'Menopause initial assessment' WHEN 
EVENT_CD LIKE '66UZ%' THEN 'Menopause monitoring NOS' WHEN 
EVENT_CD LIKE '66U2%' THEN 'Menopause follow-up assessment' WHEN 
EVENT_CD LIKE '66U6%' THEN 'HRT contraindicated' WHEN 
EVENT_CD LIKE '66U5%' THEN 'Menopause: bone density check' WHEN
EVENT_CD LIKE '66UE%' THEN 'Menopause: sexual advice' WHEN 
EVENT_CD LIKE '66UF%' THEN 'Menopause: gen counselling' WHEN 
EVENT_CD LIKE '66U4%' THEN 'Menopause: LH, FSH checked' WHEN 
EVENT_CD LIKE '66U9%' THEN 'HRT changed' WHEN
EVENT_CD LIKE '66UG%' THEN 'Patient refuses HRT' WHEN 
EVENT_CD LIKE '66UD%' THEN 'Menopause dietary advice' WHEN 
EVENT_CD LIKE '66UJ%' THEN 'Hormone replacement therapy bleed pattern - not relevant'
ELSE 'None' END AS HRTmonitored
FROM SAIL1448V.WLGP_GP_EVENT_CLEANSED_20240401 
where GNDR_CD=2)
WHERE HRTmonitored NOT LIKE 'None') WITH DATA;"
                   , sep=""))


runSQL(conn,paste0("INSERT INTO ",meno_base_complete," 
 (alf_pe, FIRST_KNOWN_MENOPAUSE_START_DT, event_cd, evidence_type, table_index, menopause_type)
SELECT alf_pe, 
MONITOR_DT AS FIRST_KNOWN_MENOPAUSE_START_DT, 
MONITOR_CD AS EVENT_CD,
HRTMONITORED AS evidence_type,
'Monitoring2024' AS table_index,
'Symptomatic_menopause' AS menopause_type from  SAILW1448V.Monitoring2024 "
                   , sep=""))


drop_table(conn,paste("",meno_symptoms ,"",sep=""))
runSQL(conn,paste0("create table ",meno_symptoms," as (SELECT * FROM (
SELECT ALF_PE, EVENT_DT as symptom_DT, EVENT_CD as symptom_CD,
CASE WHEN EVENT_CD	LIKE '1657%' THEN 'Hot flushes' WHEN 
EVENT_CD	LIKE '1512%' THEN 'Menopause' WHEN 
EVENT_CD	LIKE 'ZG61%' THEN 'Advice about the menopause' WHEN 
EVENT_CD LIKE 'K171%' THEN 'Postmenopausal atrophic urethritus' WHEN 
EVENT_CD LIKE '151K%' THEN 'Postmenopausal state' WHEN 
EVENT_CD LIKE '1583%' THEN 'H/O: post-menopausal bleeding' WHEN 
EVENT_CD LIKE '67I1%' THEN 'Advice about the menopause' WHEN 
EVENT_CD LIKE 'K171%' THEN 'Post menopausal atrophic urethritis' WHEN 
EVENT_CD LIKE 'K59B%' THEN 'Postmenopausal postcoital bleeding' WHEN 
EVENT_CD	LIKE 	'K5A20%'	THEN 'Menopausal flushing' when
EVENT_CD	LIKE 	'1672%'	THEN 'Hot flushes' when                                   
EVENT_CD	LIKE 	'K5A3%'	THEN 'Postmenopausal atrophic vaginitis' when             
EVENT_CD	LIKE 	'K5A22%'	THEN 'Menopausal headache' when
EVENT_CD	LIKE 	'N331B%'	THEN 'Postmenopausal osteoporosis with pathological fracture' when
EVENT_CD	LIKE 	'K5A23%'	THEN 'Menopausal concentration lack' WHEN
EVENT_CD	LIKE 	'K5A6%'	THEN 'Perimenopausal menorrhagia' WHEN
EVENT_CD	LIKE 	'K5A4%'	THEN 'Artificial menopause state' WHEN
EVENT_CD	LIKE 	'K5A1%'	THEN 'Postmenopausal bleeding' WHEN
EVENT_CD	LIKE 	'N3302%' THEN 'Postmenopausal osteoporosis' when
EVENT_CD	LIKE 	'K5A2z%'	THEN 'Menopausal symptoms NOS' when
EVENT_CD	LIKE 	'1583%'	THEN 'H/O: post-menopausal bleeding' WHEN
EVENT_CD	LIKE 	'K5A21%'	THEN 'Menopausal sleeplessness' WHEN
EVENT_CD	LIKE 	'Kyu9F%' THEN '[X]Other specified menopausal and perimenopausal disorders' WHEN
EVENT_CD	LIKE 	'151K%' THEN 'Postmenopausal state' WHEN
EVENT_CD	LIKE 	'K5A3%' THEN 'Atrophy of vagina' WHEN
EVENT_CD	LIKE 	'K5A5%' THEN 'Perimenopausal atrophic vaginitis' WHEN
EVENT_CD	LIKE 	'K5Ay%' THEN 'Other menopausal and postmenopausal states' WHEN
EVENT_CD	LIKE 	'K5Az%' THEN 'Menopausal and postmenopausal disorder NOS' WHEN
EVENT_CD	LIKE 	'44x2%' THEN 'Menopausal profile'
ELSE 'None' END as symptom_DESC
FROM SAIL1448V.WLGP_GP_EVENT_CLEANSED_20240401
WHERE GNDR_CD=2)
WHERE symptom_DESC NOT LIKE 'None%') WITH data;"
                   , sep=""))


runSQL(conn,paste0("INSERT INTO ",meno_base_complete," 
(alf_pe, FIRST_KNOWN_MENOPAUSE_START_DT, event_cd, evidence_type, table_index, menopause_type)
SELECT alf_pe, 
symptom_DT AS FIRST_KNOWN_MENOPAUSE_START_DT, 
symptom_CD AS event_cd,
symptom_DESC AS evidence_type,
'MENOPAUSESymptoms2024' AS table_index, 
'Symptomatic_menopause' AS menopause_type
FROM SAILW1448V.MENOPAUSESymptoms2024;
"
                   , sep=""))


# We'll need to reclassify Surgicalmenopause into Surgical_menopause to combine primary and secondary data.
runSQL(conn,paste0("UPDATE ",meno_base_complete," m
SET m.menopause_type='Surgical_menopause'
WHERE m.menopause_type='SurgicalMenopause'"
                   , sep=""))



# we don't want local oestrogen in there as premenopausal women can be prescribed it as well as women many years post-menopause.

runSQL(conn,paste0("DELETE FROM ",meno_base_complete," 
where EVIDENCE_TYPE LIKE 'Oestrogen vaginal pessaries/tablets';"
                   , sep=""))

# Some surgeries do not always result in menopause, so we will remove these

runSQL(conn,paste0("DELETE FROM ",meno_base_complete," 
  WHERE EVIDENCE_TYPE LIKE '%Unspecified vaginal excision of uterus%' 
  OR EVIDENCE_TYPE LIKE '%Unspecified abdominal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Unspecified abdominal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Other specified abdominal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Other specified vaginal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterocolpectomy NEC%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterectomy and excision of periuterine tissue NEC%'
  OR EVIDENCE_TYPE LIKE '%Abdominal hysterocolpectomy NEC%'
  OR EVIDENCE_TYPE LIKE '%Abdominal hysterectomy and excision of periuterine tissue NEC%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterocolpectomy and excision of periuterine tissue%'
  OR EVIDENCE_TYPE LIKE '%Abdominal hysterocolpectomy and excision of periuterine tissue%'
  OR EVIDENCE_TYPE LIKE '%Bilateral salpingectomy NEC%' 
  OR EVIDENCE_TYPE LIKE '%Salpingectomy of remaining solitary fallopian tube NEC%'
  OR EVIDENCE_TYPE LIKE '%Excision of accessory uterus%'
  OR EVIDENCE_TYPE LIKE '%Abdominal hysterocolpectomy and excision of periuterine tissue%'
  OR EVIDENCE_TYPE LIKE '%Unspecified vaginal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Vaginal excision of uterus NOS%'
  OR EVIDENCE_TYPE LIKE '%Laparoscopic vaginal hysterectomy%'
  OR EVIDENCE_TYPE LIKE '%Other specified abdominal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Abdominal hysterocolpectomy NEC%'
  OR EVIDENCE_TYPE LIKE '%Laparoscopic hysterectomy%'
  OR EVIDENCE_TYPE LIKE '%Abdominal hysterectomy and excision of periuterine tissue NEC%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterocolpectomy NEC%'
  OR EVIDENCE_TYPE LIKE '%Abdominal excision of uterus NOS%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterocolpectomy NEC%'
  OR EVIDENCE_TYPE LIKE '%Salpingectomy of remaining solitary fallopian tube NEC%'
  OR EVIDENCE_TYPE LIKE '%Laparoscopic subtotal hysterectomy%'
  OR EVIDENCE_TYPE LIKE '%Subtotal abdominal hysterectomy%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterectomy NEC%'
  OR EVIDENCE_TYPE LIKE '%h/o: hysterectomy%'
  OR EVIDENCE_TYPE LIKE '%no smear - benign hysterectomy%'
  OR EVIDENCE_TYPE LIKE '%Abdominal hysterectomy & excision of periuterine tissue NEC%'
  OR EVIDENCE_TYPE LIKE '%Total abdominal hysterectomy NEC%'
  OR EVIDENCE_TYPE LIKE '%Other specified vaginal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterectomy and excision of periuterine tissue NEC%'
  OR EVIDENCE_TYPE LIKE '%Abdominal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Unspecified abdominal excision of uterus%'
  OR EVIDENCE_TYPE LIKE '%Post hysterectomy vaginal vault prolapse%'
  OR EVIDENCE_TYPE LIKE '%Vaginal hysterocolpectomy and excision of periuterine tissue%'
;"
                   , sep=""))


# we need to add constraints in terms of age- as some surgeries may have happened well after natural menopause 

# If first menopause evidence was surgical_meno, and 
#1) they were older than 55 when surgery occured, and
#2) they did not have any other meno codes after their surgery (e.g. T or O/HRT, symptoms etc.) 
#delete these women from the table (as we can't be sure of their meno index date)

# delete if only given menopause_type is 'Surgical_meno'
# AND if min age is >55

runSQL(conn,paste0("DELETE FROM ",meno_base_complete," 
WHERE ALF_PE IN 
(SELECT DISTINCT m.ALF_PE
FROM sail1448v.WLGP_GP_EVENT_CLEANSED_20240401 wgec
inner JOIN 
sailw1448v.MENOPAUSE_BASE_COHORT_complete m
ON m.alf_pe=wgec.alf_pe
GROUP BY m.ALF_PE, menopause_type
having min(YEAR(m.first_known_menopause_start_dt)-year(wgec.WOB))>55
AND COUNT(DISTINCT MENOPAUSE_TYPE)=1
AND MENOPAUSE_TYPE='Surgical_menopause'
);"
                   , sep=""))

# We'll need to reclassify Symptomatic_menopause into Early_menopause for women who had a Symptomatic_menopause code before 40.
runSQL(conn,paste0("UPDATE ",meno_base_complete," m
SET m.menopause_type='Early_menopause'
WHERE m.menopause_type='Symptomatic_menopause'
AND m.alf_pe IN 
(SELECT wgec.alf_pe FROM sail1448v.WLGP_GP_EVENT_CLEANSED_20240401 wgec
WHERE (YEAR(m.first_known_menopause_start_dt)-year(wgec.WOB)) <40);"
                   , sep=""))


#delete if min age is not between 18 (adults only) and 65 (negate impact of age, plus after age 65 menopause age likely occured many years before record).

runSQL(conn,paste0("DELETE FROM ",meno_base_complete," 
WHERE ALF_PE IN 
(SELECT DISTINCT 
  m.ALF_PE
  FROM sail1448v.WLGP_GP_EVENT_CLEANSED_20240401 wgec
  inner JOIN 
  sailw1448v.MENOPAUSE_BASE_COHORT_complete m
  ON m.alf_pe=wgec.alf_pe
  GROUP BY m.ALF_PE
  having min(YEAR(m.first_known_menopause_start_dt)-year(wgec.WOB))<18
  or max(YEAR(m.first_known_menopause_start_dt)-year(wgec.WOB))>65)
;"
                   , sep=""))


# Delete those born before 1990

runSQL(conn,paste0("DELETE FROM ",meno_base_complete," 
WHERE ALF_PE IN 
(SELECT DISTINCT 
m.ALF_PE
FROM sail1448v.WLGP_GP_EVENT_CLEANSED_20240401 wgec
inner JOIN 
sailw1448v.MENOPAUSE_BASE_COHORT_complete m
ON m.alf_pe=wgec.alf_pe
GROUP BY m.ALF_PE
having min(YEAR(wgec.WOB))<1900)
;"
                   , sep=""))

# Remove menopause start dates before 1996 and after 2023
runSQL(conn,paste0("DELETE FROM ",meno_base_complete," 
WHERE ALF_PE IN 
(SELECT DISTINCT 
m.ALF_PE
FROM  sailw1448v.MENOPAUSE_BASE_COHORT_complete m
GROUP BY m.ALF_PE
having min(YEAR(m.first_known_menopause_start_dt))<1996
or max(YEAR(m.first_known_menopause_start_dt))>2023)"
                   , sep=""))


# Count totals in each group

T <- runSQL(conn,paste0("SELECT count (DISTINCT ALF_PE) AS count, menopause_type  FROM 
(SELECT DISTINCT ALF_PE, menopause_type,
  ROW_NUMBER () OVER (PARTITION BY ALF_PE ORDER BY first_known_menopause_start_dT) AS rn
  FROM sailw1448v.MENOPAUSE_BASE_COHORT_complete
) AS first_instance 
WHERE rn=1
GROUP BY menopause_type
ORDER BY count DESC;"
                   , sep=""))

# create a menopause age table

drop_table(conn,paste("",meno_age ,"",sep=""))
runSQL(conn,paste0("create table ",meno_age," as 
(SELECt DISTINCT m.ALF_PE,  
  min(YEAR(m.first_known_menopause_start_dt)-year(wgec.WOB)) AS age_at_meno
  FROM sailw1448v.MENOPAUSE_BASE_COHORT_complete m
  INNER JOIN 
  sail1448v.WLGP_GP_EVENT_CLEANSED_20240401 wgec
  ON m.alf_pe=wgec.alf_pe
  GROUP BY m.ALF_PE) WITH DATA;"
                   , sep=""))

# create a menopause start date table

drop_table(conn,paste("",meno_date ,"",sep=""))
runSQL(conn,paste0("create table ",meno_date," as (
SELECT DISTINCT alf_pe, min(first_known_menopause_start_dt) as FIRST_KNOWN_MENOPAUSE_START_DT FROM sailw1448v.MENOPAUSE_BASE_COHORT_complete
GROUP BY ALF_PE 
ORDER BY ALF_PE, first_known_menopause_start_dt) WITH DATA;"
                   , sep=""))


# Now we can create the meno demographics table

drop_table(conn,paste("",meno_demographics ,"",sep=""))
runSQL(conn,paste0("create table ",meno_demographics,"
AS (SELECT m.ALF_PE,  
wgec.age_at_meno, 
dep.TOWNSEND_2011_QUINTILE, 
dep.END_DATE,
CASE WHEN m.TABLE_INDEX	LIKE 	'SURGICAL%' THEN 'Y' ELSE 'N' END as HAD_SURGERY,
CASE WHEN m.TABLE_INDEX	LIKE 	'HRT%' OR m.TABLE_INDEX	LIKE 'Monitoring%'  THEN 'Y' ELSE 'N' END as TAKING_HRT,
CASE WHEN m.TABLE_INDEX	LIKE 	'%Symptoms%' THEN 'Y' ELSE 'N' END as REPORTED_SYMPTOMS,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Ooph%' OR m.EVIDENCE_TYPE 	LIKE 	'Salping%' OR m.EVIDENCE_TYPE 	LIKE 	'Abdominal%' or m.EVIDENCE_TYPE 	LIKE 'Radical%' or m.EVIDENCE_TYPE 	LIKE 'radical%' OR m.EVIDENCE_TYPE 	LIKE '%bilat %' OR m.EVIDENCE_TYPE 	LIKE 	'%excision%' OR m.EVIDENCE_TYPE 	LIKE 	'Bilat%'  THEN 'Y' ELSE 'N' END as BILATERAL_OOPHORECTOMY,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'%hysterect%' THEN 'Y' ELSE 'N' END as WITHHYSTERECTOMY,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Combined patches%' THEN 'Y' ELSE 'N' END AS COMBINED_PATCHES,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Combined tablets%' THEN 'Y' ELSE 'N' END AS COMBINED_TABLETS,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Oestrogen Patches%' THEN 'Y' ELSE 'N' END AS OESTROGEN_PATCHES,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Oestrogen gels%' THEN 'Y' ELSE 'N' END AS OESTROGEN_GELS,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Oestrogen implants%' THEN 'Y' ELSE 'N' END AS OESTROGEN_IMPLANTS,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Oestrogen nasal sprays%' THEN 'Y' ELSE 'N' END AS OESTROGEN_NASAL,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Oestrogen tablets%' THEN 'Y' ELSE 'N' END AS OESTROGEN_TABLETS,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Testosterone%' THEN 'Y' ELSE 'N' END AS TESTOSTERONE,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Hot flushes%' OR m.EVIDENCE_TYPE 	LIKE 	'Menopausal flushing%' THEN 'Y' ELSE 'N' END AS HOT_FLUSHES,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Menopausal concentration lack%' THEN 'Y' ELSE 'N' END AS Menopause_cognition,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Menopausal headache%' THEN 'Y' ELSE 'N' END AS Menopause_headache,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Menopausal sleeplessness%' THEN 'Y' ELSE 'N' END AS Menopause_sleep,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'Perimenopausal menorr%' THEN 'Y' ELSE 'N' END AS Peri_menorrhagia,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'%sexual%' THEN 'Y' ELSE 'N' END AS meno_sexual_symptoms,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'%atrophic%' THEN 'Y' ELSE 'N' END AS meno_Vaginal_atrophy,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'%bleeding%' THEN 'Y' ELSE 'N' END AS postmeno_bleeding,
CASE WHEN m.EVIDENCE_TYPE 	LIKE 	'%osteoporosis%' THEN 'Y' ELSE 'N' END AS postmeno_osteoporosis,
m.MENOPAUSE_TYPE, 
m.EVIDENCE_TYPE,
m.FIRST_KNOWN_MENOPAUSE_START_DT 
FROM sailw1448v.MENOPAUSE_BASE_COHORT_complete m
LEFT JOIN 
sailw1448v.meno_age wgec
ON m.alf_pe=wgec.alf_pe
LEFT JOIN 
(SELECT * FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY alf_pe ORDER BY end_date desc) AS N 
FROM SAIL1448V.WDSD_SINGLE_CLEAN_GEO_CHAR_LSOA2011_20240701)
WHERE N=1) AS dep
ON m.ALF_PE =dep.ALF_PE
)WITH data;",sep=""))

# Count totals by type again for consistency check
T2 <- runSQL(conn,paste0("SELECT count (DISTINCT ALF_PE) AS count, menopause_type  FROM 
(SELECT DISTINCT ALF_PE, menopause_type,
  ROW_NUMBER () OVER (PARTITION BY ALF_PE ORDER BY first_known_menopause_start_dT) AS rn
  FROM sailw1448v.MENO_demographics
) AS first_instance 
WHERE rn=1
GROUP BY menopause_type
ORDER BY count DESC;"
                        , sep=""))


