# Iceberg 1.11 部署笔记


> **关键说明**
> 1. Iceberg不是独立服务，没有守护进程，部署Iceberg就是将Iceberg的runtime.jar放进Spark/Flink的类路径并配置Catalog
> 2. `172.16.15.15`这台机器定位为客户端，不运行HDFS、Yarn服务，只负责提交作业
> 3. Iceberg使用HiveMetaStore作为Catalog，数据文件存放再集群HDFS上

## 一、部署机器说明

| 主机           | 服务                          |
| ------------ | --------------------------- |
| hadoop102    | HDFS、YARN、HIVE、JDK8         |
| hadoop103    | HDFS、YARN、JDK8              |
| hadoop104    | HDFS、YARN、JDK8              |
| 172.16.15.15 | Spark4.1.3、Flink2.1.3、JDK17 |

**文档中涉及的部署默认是在172.16.15.15上执行**

## 二、环境准备

### 2.1 JDK17

安装JDK17，并将安装包分发到其他的节点

### 2.2 拷贝Hadoop客户端

将hadoop102上的hadoop拷贝到15上：`scp -r hadoop102:/opt/module/hadoop-3.3.4 /data`

### 2.3 拷贝Hive配置文件

创建文件夹：`mkdir -r /data/hive/conf`

拷贝配置文件：`cp hadoop102:/opt/module/hive/conf/hive-site.xml /data/hive/conf/`

### 2.4 配置环境变量

```bash
# JDK
export JAVA_HOME=/data/jdk17
export PATH=$JAVA_HOME/bin:$PATH

export HADOOP_HOME=/data/hadoop-3.3.4
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HIVE_CONF_DIR=/data/hive/conf
# 供 Spark/Flink 使用的 Hadoop 类路径
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
```

## 三、部署Spark4.1.3 + Iceberg

1. 解压安装包，解压之后路径`/data/spark-4.1.3`
2. 下载并上传Iceberg Spark Runtime，放入Spark jars目录 `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`
3. 添加环境变量
```bash
export SPARK_HOME=/data/spark-4.1.3
export PATH=$SPARK_HOME/bin:$PATH
```
4. 配置spark-env.sh
```bash
export JAVA_HOME=/data/jdk17
export HADOOP_CONF_DIR=/data/hadoop-3.3.4/etc/hadoop
export YARN_CONF_DIR=/data/hadoop-3.3.4/etc/hadoop
# 若向 YARN 提交出现类冲突，取消下行注释，改用集群 Hadoop 客户端
# export SPARK_DIST_CLASSPATH=$(/data/hadoop-3.3.4/bin/hadoop classpath)
```
5. 配置spark-default.conf (这里配置的就是Iceberg的核心配置)
```properties
# ---------- 集群提交 ----------
spark.master                              yarn
spark.submit.deployMode                   client
spark.eventLog.enabled                    true
spark.eventLog.dir                        hdfs://hadoop102:8020/sparkHistory
spark.serializer                          org.apache.spark.serializer.KryoSerializer

# ---------- Iceberg SQL 扩展（CALL 存储过程等需要）----------
spark.sql.extensions                      org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# ---------- Iceberg HiveCatalog，catalog 名字为 iceberg ----------
spark.sql.catalog.iceberg                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type            hive
spark.sql.catalog.iceberg.uri             thrift://hadoop102:9083
spark.sql.catalog.iceberg.warehouse       hdfs://hadoop102:8020/warehouse/iceberg
spark.sql.catalog.iceberg.default-namespace  default
spark.sql.catalog.iceberg.hadoop.fs.defaultFS  hdfs://hadoop102:8020

spark.yarn.appMasterEnv.JAVA_HOME    /opt/module/jdk17
spark.executorEnv.JAVA_HOME          /opt/module/jdk17
```
6. `hdfs dfs -mkdir -p /sparkHistory`

## 四、部署Flink2.1.3 + Iceberg

1. 解压安装包，解压之后路径`/data/flink-2.1.3`
2. 下载并上传Iceberg Flink Runtime，放入Flink lib目录 `iceberg-flink-runtime-2.1-1.11.0.jar`
3. 添加环境变量
```bash
export FLINK_HOME=/data/flink-2.1.3
export PATH=$FLINK_HOME/bin:$PATH
```
4. 上传Hive客户端依赖 `flink-sql-connector-hive-3.1.3_2.12-3.0.0-1.20.jar`
5. 上传MR依赖 `hadoop-mapreduce-client-core-3.3.4.jar`
6. 配置flink-env.sh
```bash
export JAVA_HOME=/data/jdk17
export HADOOP_CLASSPATH=`/data/hadoop-3.3.4/bin/hadoop classpath`
export HADOOP_CONF_DIR=/data/hadoop-3.3.4/etc/hadoop
```
7. 配置config.yaml
```yaml
env:
  java:
    home: /data/jdk17
```

## 五、验证

### 5.1 Spark验证

```bash
spark-sql --master yarn
```

```sql
CREATE DATABASE IF NOT EXISTS iceberg.iceberg_db;
CREATE TABLE iceberg.iceberg_db.sample (
  id BIGINT, name STRING, ts TIMESTAMP
) USING iceberg
TBLPROPERTIES ('format-version'='2');

-- 写入 + 查询
INSERT INTO iceberg.iceberg_db.sample VALUES (1,'alice',current_timestamp()),(2,'bob',current_timestamp());
SELECT * FROM iceberg.iceberg_db.sample;

-- 查看快照与数据文件（Iceberg 元数据表）
SELECT * FROM iceberg.iceberg_db.sample.snapshots;
SELECT file_path, record_count FROM iceberg.iceberg_db.sample.files;

-- 时间旅行（snapshot_id 取自上一步查询结果）
SELECT * FROM iceberg.iceberg_db.sample VERSION AS OF <snapshot_id>;

-- 调用维护存储过程（小文件合并）
CALL iceberg.system.rewrite_data_files('iceberg_db.sample');
```

### 5.2 Flink验证

```bash
yarn-session.sh
sql-client.sh
```

```sql
CREATE CATALOG iceberg_hive WITH (
  'type'            = 'iceberg',
  'catalog-type'    = 'hive',
  'uri'             = 'thrift://hadoop102:9083',
  'clients'         = '5',
  'property-version'= '1',
  'warehouse'       = 'hdfs://hadoop102:8020/warehouse/iceberg'
);

USE CATALOG iceberg_hive;

CREATE DATABASE IF NOT EXISTS flink_db;
CREATE TABLE flink_db.flink_sample (id BIGINT, data STRING);
INSERT INTO flink_db.flink_sample VALUES (1, 'a'), (2, 'b');
SELECT * FROM flink_db.flink_sample;
```
