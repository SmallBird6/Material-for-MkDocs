# 一、迁移方式

## 1.1 migrate

官方提供的原生迁移方式之一，用来进行大规模迁移，原地接管模式

原理：
- 物理数据文件不动，不复制
- 直接读取Hive表再HMS里的元数据，生成Iceberg的metadata.json文件
- 最后将原Hive表重命名`<table_name>_BACKUP_`（作为备份），并用原表名新建一张接管物理文件的Iceberg表

不适用于此次迁移验证，我们需要保留Hive的数据和元数据，Hive还要可用

## 1.2 snapshot

官方提供的只读快照方式

原理：
- Hive原表不动，再Iceberg侧新建一张共享引用源表数据文件的快照表

不适用于此次迁移验证，之后Hive的操作会影响使用，未分离开

## 1.3 CTAS

数据复制，最慢的迁移方式，但是适用于此次迁移验证

原理：
- 执行SQL，`CREATE TABLE ... AS SELECT` 把Hive表数据**复制**成Iceberg表的新文件，写入`/warehouse/iceberg` 新位置。源表全程零改动，迁移后两套数据完全独立、互不影响
- 物理复制全量数据

# 二、迁移

进行少量表迁移认证，Spark操作：

## 2.1 创建Iceberg目标数据库

```sql
CREATE DATABASE IF NOT EXISTS iceberg.dim_mt;
CREATE DATABASE IF NOT EXISTS iceberg.ods_mt;
CREATE DATABASE IF NOT EXISTS iceberg.dwd_mt;
CREATE DATABASE IF NOT EXISTS iceberg.dws_mt;
```

## 2.2 CTAS 建表与数据复制 SQL

```sql
-- 1.1 dim_icd10_code
CREATE TABLE IF NOT EXISTS iceberg.dim_mt.dim_icd10_code
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dim.dim_icd10_code;

-- 1.2 dim_loinc_code
CREATE TABLE IF NOT EXISTS iceberg.dim_mt.dim_loinc_code
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dim.dim_loinc_code;

-- 2.1 ods_mt_patient_info
CREATE TABLE IF NOT EXISTS iceberg.ods_mt.ods_mt_patient_info
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_ods.ods_mt_patient_info;

-- 2.2 ods_mt_medical_record_first_fee
CREATE TABLE IF NOT EXISTS iceberg.ods_mt.ods_mt_medical_record_first_fee
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_ods.ods_mt_medical_record_first_fee;

-- 2.3 ods_mt_medical_record_first_disease
CREATE TABLE IF NOT EXISTS iceberg.ods_mt.ods_mt_medical_record_first_disease
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_ods.ods_mt_medical_record_first_disease;

-- 3.1 dwd_mt_medical_record_first_fee
CREATE TABLE IF NOT EXISTS iceberg.dwd_mt.dwd_mt_medical_record_first_fee
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dwd.dwd_mt_medical_record_first_fee;

-- 3.2 dwd_mt_patient_info
CREATE TABLE IF NOT EXISTS iceberg.dwd_mt.dwd_mt_patient_info
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dwd.dwd_mt_patient_info;

-- 3.3 dwd_mt_medical_record_first_disease
CREATE TABLE IF NOT EXISTS iceberg.dwd_mt.dwd_mt_medical_record_first_disease
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dwd.dwd_mt_medical_record_first_disease;

-- 4.1 dws_outcome_features
CREATE TABLE IF NOT EXISTS iceberg.dws_mt.dws_outcome_features
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dws.dws_outcome_features;

-- 4.2 dws_patient_demographics
CREATE TABLE IF NOT EXISTS iceberg.dws_mt.dws_patient_demographics
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dws.dws_patient_demographics;

-- 4.3 tmp_clinical_fill_pid
CREATE TABLE IF NOT EXISTS iceberg.dws_mt.tmp_clinical_fill_pid
USING iceberg TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM spark_catalog.mt_dws.tmp_clinical_fill_pid;
```

## 2.3 验证

验证Hive表的行数和Iceberg表的行数
