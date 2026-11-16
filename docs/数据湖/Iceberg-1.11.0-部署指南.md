# Iceberg 1.11.0 部署文档（hadoop105 客户端节点）

> 集群环境：CentOS 7 · Hadoop(HDFS+YARN) 3.1.3 · Hive 3.1.3 · Spark 4.1.3 · Flink 2.1.3 · Iceberg 1.11.0
> 文档日期：2026-07 · 版本号与下载地址均经官方渠道核实

---

## 1. 总体说明

### 1.1 节点角色

| 节点 | 已有服务 | 本次部署 |
|---|---|---|
| hadoop102 | HDFS(NameNode/DataNode)、YARN、Hive 3.1.3(Metastore) | 不动 |
| hadoop103 | HDFS、YARN | 不动 |
| hadoop104 | HDFS、YARN | 不动 |
| hadoop105 | 无 | JDK 17、Hadoop/Hive 客户端配置、Spark 4.1.3、Flink 2.1.3、Iceberg 1.11.0 |

### 1.2 关键认知

- **Iceberg 不是独立服务**，没有守护进程。"部署 Iceberg" = 把 Iceberg 的 runtime jar 放进 Spark/Flink 的类路径，并配置 Catalog。
- hadoop105 定位是**客户端/网关节点**：不跑 DataNode/NodeManager，只负责向 hadoop102~104 的集群提交作业。
- Iceberg 元数据存放在 hadoop102 的 **Hive Metastore**（HiveCatalog，`thrift://hadoop102:9083`），数据文件存放在集群 HDFS 上。Hive/Hadoop 均无需改动或重启。

### 1.3 软件版本清单（已核实）

| 软件 | 版本 | 说明 |
|---|---|---|
| JDK | 17 (Temurin) | Spark 4.1 强制 Java 17+；Flink 2.1 支持 11/17；Iceberg 1.11.0 已移除 Java 11 |
| Hadoop 客户端 | 与集群一致（3.1.3） | 直接从 hadoop102 拷贝，保证版本匹配 |
| Spark | 4.1.3 (`spark-4.1.3-bin-hadoop3.tgz`) | 自带 Hive 2.3.10 客户端 jar，可直连 HMS 3.1.3 |
| Flink | 2.1.3 (`flink-2.1.3-bin-scala_2.12.tgz`) | Scala 2.12（Iceberg flink-runtime 按 2.12 编译） |
| Iceberg Spark runtime | `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` | Spark 侧 Iceberg 全部能力 |
| Iceberg Flink runtime | `iceberg-flink-runtime-2.1-1.11.0.jar` | Flink 侧 Iceberg 全部能力 |
| Flink Hive 依赖 | `flink-sql-connector-hive-3.1.3_2.12` | 详见 5.3 节版本说明 |

> 兼容性依据：Iceberg 官方文档 "HiveCatalog integration supports Hive 2.3.10 or 3.1.3 or later"；Spark 4.1 文档 `spark.sql.hive.metastore.version` 支持 3.0.0–3.1.3；Flink 2.1 文档支持 Hive 3.1.0–3.1.3。

---

## 2. 系统环境准备（hadoop105）

### 2.1 hosts 与基础配置

```bash
# 四台机器主机名写入 /etc/hosts（IP 按实际环境替换）
cat >> /etc/hosts <<'EOF'
192.168.10.102 hadoop102
192.168.10.103 hadoop103
192.168.10.104 hadoop104
192.168.10.105 hadoop105
EOF

# 关闭防火墙（或按需放行 8020/9083/8088 等端口）
systemctl stop firewalld && systemctl disable firewalld

# 时间同步（与集群保持一致）
yum install -y ntpdate
ntpdate ntp.aliyun.com

# 创建部署用户与目录（沿用集群风格 /opt/module，可自行调整）
useradd hadoop && echo 'hadoop:123456' | chpasswd
mkdir -p /opt/module /opt/software
chown -R hadoop:hadoop /opt/module /opt/software
```

后续操作除特别说明外，均以 `hadoop` 用户执行（`su - hadoop`）。

### 2.2 安装 JDK 17

```bash
cd /opt/software
# 方式一：下载 Adoptium Temurin 17（推荐，版本固定）
wget -O OpenJDK17.tar.gz \
  "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse"
tar -zxvf OpenJDK17.tar.gz -C /opt/module/
mv /opt/module/jdk-17* /opt/module/jdk17

# 方式二：若 yum 源提供 java-17-openjdk-devel，也可 yum 安装
# yum install -y java-17-openjdk-devel
```

配置环境变量 `/etc/profile.d/bigdata.sh`（全篇环境变量都放这里）：

```bash
cat > /etc/profile.d/bigdata.sh <<'EOF'
# JDK
export JAVA_HOME=/opt/module/jdk17
export PATH=$JAVA_HOME/bin:$PATH
EOF
source /etc/profile.d/bigdata.sh
java -version   # 应显示 openjdk version "17.x.x"
```

---

### 2.3 分发 JDK 17 到集群节点（YARN 容器必须）

> **为什么必须做这一步**：Spark 4.1 的 class 文件版本是 61（Java 17 编译），而集群节点默认是 JDK 8（最高只认 52）。`--master yarn` 时，ApplicationMaster / Executor 容器跑在 hadoop102~104 上，默认用节点的 JDK 8 启动，必然报 `UnsupportedClassVersionError`。

```bash
# 将 hadoop105 上装好的 JDK 17 按相同路径分发到所有集群节点
for i in 102 103 104; do
  scp -r /opt/module/jdk17 hadoop$i:/opt/module/
done
```

> Hadoop/Hive 自身继续运行在原 JDK 8 上，**不受影响**——只有 Spark/Flink 的 YARN 容器会被显式指向 JDK 17（见 4.3、5.4 节配置）。

## 3. 下发 Hadoop / Hive 客户端配置

hadoop105 需要集群的配置文件与 Hadoop 客户端程序，用于访问 HDFS、向 YARN 提交作业。

### 3.1 拷贝 Hadoop 客户端（保证与集群版本一致）

```bash
# 从 hadoop102 直接拷贝集群在用的 Hadoop（路径按实际安装路径调整）
scp -r hadoop102:/opt/module/hadoop-3.1.3 /opt/module/

# 从 hadoop102 拷贝 Hive 的配置文件（只需要配置，不需要整套 Hive）
mkdir -p /opt/module/hive-client-conf
scp hadoop102:/opt/module/hive/conf/hive-site.xml /opt/module/hive-client-conf/
```

> 若未配免密，先执行 `ssh-keygen` + `ssh-copy-id hadoop102`，或改用下载方式：
> `wget https://archive.apache.org/dist/hadoop/common/hadoop-3.1.3/hadoop-3.1.3.tar.gz`

### 3.2 环境变量

向 `/etc/profile.d/bigdata.sh` 追加：

```bash
cat >> /etc/profile.d/bigdata.sh <<'EOF'
# Hadoop 客户端
export HADOOP_HOME=/opt/module/hadoop-3.1.3
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HIVE_CONF_DIR=/opt/module/hive-client-conf
# 供 Spark/Flink 使用的 Hadoop 类路径
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
EOF
source /etc/profile.d/bigdata.sh
```

### 3.3 验证集群连通性

```bash
hdfs dfs -ls /          # 能列出集群 HDFS 根目录
yarn node -list         # 能列出 hadoop102~104 的 NodeManager
```

---

## 4. 部署 Spark 4.1.3 + Iceberg

### 4.1 安装 Spark

```bash
cd /opt/software
wget https://archive.apache.org/dist/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz
tar -zxvf spark-4.1.3-bin-hadoop3.tgz -C /opt/module/
ln -s /opt/module/spark-4.1.3-bin-hadoop3 /opt/module/spark

# 下载 Iceberg Spark runtime（45.7 MB），放入 Spark jars 目录
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
  -P /opt/module/spark/jars/
```

> Spark 官方发行包自带 Hive 2.3.10 客户端 jar，Iceberg 的 HiveCatalog 直接复用它通过
> Thrift 连接 hadoop102 的 HMS 3.1.3，**无需额外拷贝 Hive jar**。

追加环境变量：

```bash
cat >> /etc/profile.d/bigdata.sh <<'EOF'
# Spark
export SPARK_HOME=/opt/module/spark
export PATH=$SPARK_HOME/bin:$PATH
EOF
source /etc/profile.d/bigdata.sh
```

### 4.2 配置 spark-env.sh

```bash
cd /opt/module/spark/conf
cp spark-env.sh.template spark-env.sh
cat >> spark-env.sh <<'EOF'
export JAVA_HOME=/opt/module/jdk17
export HADOOP_CONF_DIR=/opt/module/hadoop-3.1.3/etc/hadoop
export YARN_CONF_DIR=/opt/module/hadoop-3.1.3/etc/hadoop
# 若向 YARN 提交出现类冲突，取消下行注释，改用集群 Hadoop 客户端
# export SPARK_DIST_CLASSPATH=$(/opt/module/hadoop-3.1.3/bin/hadoop classpath)
EOF
```

### 4.3 配置 spark-defaults.conf（Iceberg Catalog 核心配置）

```bash
cat > /opt/module/spark/conf/spark-defaults.conf <<'EOF'
# ---------- 集群提交 ----------
spark.master                              yarn
spark.submit.deployMode                   client
spark.eventLog.enabled                    true
spark.eventLog.dir                        hdfs://hadoop102:8020/spark-history
spark.yarn.historyServer.address          hadoop105:18080
spark.serializer                          org.apache.spark.serializer.KryoSerializer
spark.yarn.appMasterEnv.JAVA_HOME         /opt/module/jdk17
spark.executorEnv.JAVA_HOME               /opt/module/jdk17

# ---------- Iceberg SQL 扩展（CALL 存储过程等需要）----------
spark.sql.extensions                      org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# ---------- Iceberg HiveCatalog，catalog 名字为 iceberg ----------
spark.sql.catalog.iceberg                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type            hive
spark.sql.catalog.iceberg.uri             thrift://hadoop102:9083
spark.sql.catalog.iceberg.warehouse       hdfs://hadoop102:8020/warehouse/iceberg
spark.sql.catalog.iceberg.default-namespace  default
spark.sql.catalog.iceberg.hadoop.fs.defaultFS  hdfs://hadoop102:8020
EOF
```

> 说明：
> - `spark.eventLog.dir` 需先创建：`hdfs dfs -mkdir -p /spark-history`
> - `warehouse` 使用独立目录 `/warehouse/iceberg`，与 Hive 数仓目录隔离，便于管理

---

## 5. 部署 Flink 2.1.3 + Iceberg

### 5.1 安装 Flink

```bash
cd /opt/software
wget https://archive.apache.org/dist/flink/flink-2.1.3/flink-2.1.3-bin-scala_2.12.tgz
tar -zxvf flink-2.1.3-bin-scala_2.12.tgz -C /opt/module/
ln -s /opt/module/flink-2.1.3 /opt/module/flink

# 下载 Iceberg Flink runtime（37.3 MB），放入 Flink lib 目录
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar \
  -P /opt/module/flink/lib/
```

### 5.2 环境变量与 JVM 配置

```bash
cat >> /etc/profile.d/bigdata.sh <<'EOF'
# Flink
export FLINK_HOME=/opt/module/flink
export PATH=$FLINK_HOME/bin:$PATH
EOF
source /etc/profile.d/bigdata.sh

# flink-env.sh
cat >> /opt/module/flink/conf/flink-env.sh <<'EOF'
export JAVA_HOME=/opt/module/jdk17
export HADOOP_CLASSPATH=`/opt/module/hadoop-3.1.3/bin/hadoop classpath`
export HADOOP_CONF_DIR=/opt/module/hadoop-3.1.3/etc/hadoop
EOF
```

### 5.3 为 Flink 提供 Hive 客户端依赖

> 已实测验证：Iceberg 的 runtime jar 内只含 `org.apache.iceberg.hive.*`（HiveCatalog 实现），
> **不含** Hive Metastore 客户端类（`org.apache.hadoop.hive.metastore.*`），需另行提供。

**方案 A（推荐，官方方式）**：把 Flink 的 Hive SQL connector 放进 `lib/`：

```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/3.0.0-1.20/flink-sql-connector-hive-3.1.3_2.12-3.0.0-1.20.jar \
  -P /opt/module/flink/lib/
```

> ⚠️ 版本现状（已核实 Maven Central，2026-07）：Flink 2.1 官方文档引用的
> `flink-sql-connector-hive-3.1.3_2.12:2.1.1` **尚未发布到 Maven 仓库**，当前最新可用为
> `3.0.0-1.20`（Hive 连接器已独立发版，面向 Flink 1.20 构建）。该 uber jar 自带 Hive 3.1.3
> 全套类，Iceberg 的 HiveCatalog 只借用其中的 HMS 客户端类即可工作；若你的 SQL 作业
> 还需要使用 Flink 自身的 HiveCatalog / Hive 方言并出现异常，改用方案 B。

**方案 B（备选，最贴近集群）**：从 hadoop102 拷贝 Hive 3.1.3 客户端 jar：

```bash
scp hadoop102:/opt/module/hive/lib/{hive-metastore-3.1.3.jar,hive-exec-3.1.3.jar,hive-common-3.1.3.jar,libfb303-0.9.3.jar,libthrift-0.9.3.jar} \
  /opt/module/flink/lib/
```

方案 A、B 二选一，**不要同时放**，避免类冲突。

### 5.4 配置 config.yaml（Flink 2.x 新配置格式）

```bash
cat >> /opt/module/flink/conf/config.yaml <<'EOF'
jobmanager:
  memory:
    process:
      size: 2048m
taskmanager:
  memory:
    process:
      size: 4096m
  numberOfTaskSlots: 4
parallelism:
  default: 2
execution:
  checkpointing:
    interval: 60s
rest:
  bind-address: hadoop105
env:
  java:
    home: /opt/module/jdk17
EOF
```

---

## 6. Hive / HDFS 侧准备（在 hadoop102 执行一次）

```bash
# 1. 确认 Hive Metastore 已启动并监听 9083
nohup hive --service metastore > /opt/module/hive/logs/metastore.log 2>&1 &
netstat -ntlp | grep 9083

# 2. 创建 Iceberg 独立仓库目录并授权（测试环境直接 777，生产按用户细粒度授权）
hdfs dfs -mkdir -p /warehouse/iceberg
hdfs dfs -chmod -R 777 /warehouse/iceberg

# 3. 创建 Spark 历史日志目录
hdfs dfs -mkdir -p /spark-history
hdfs dfs -chmod 777 /spark-history
```

> 说明：Iceberg 的 HiveCatalog 只是在 HMS 里注册表元数据（`table_type=ICEBERG`），
> **不需要 Hive 服务端做任何改造**，Hive 3.1.3 原生满足 Iceberg 1.11.0 的要求。

---

## 7. 部署验证

### 7.1 Spark 验证（hadoop105）

```bash
spark-sql --master yarn
```

```sql
-- 建库建表（V2 格式，兼容性最好）
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

同时验证 HDFS 上生成了标准 Iceberg 目录结构：

```bash
hdfs dfs -ls /warehouse/iceberg/iceberg_db/sample/
# 应看到 data/ 与 metadata/ 两个目录，metadata 下有 v1.metadata.json、snap-*.avro 等
```

### 7.2 Flink 验证（hadoop105）

```bash
# 启动 SQL Client（embedded 模式，本地验证用）
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

生产使用建议改为 YARN 会话模式：

```bash
yarn-session.sh -d -jm 1024m -tm 2048m
sql-client.sh   # 作业提交到 YARN 会话集群
```

### 7.3 跨引擎互通验证（核心价值）

- **Flink 写、Spark 读**：在 spark-sql 中执行 `SELECT * FROM iceberg.flink_db.flink_sample;`
- **Spark 写、Flink 读**：在 flink sql-client 中执行 `SELECT * FROM iceberg_db.sample;`

两侧都能看到对方写入的数据，说明共享 HMS 元数据 + HDFS 数据的链路完全打通。

### 7.4 Hive 侧确认（hadoop102）

```bash
beeline -u jdbc:hive2://hadoop102:10000 -e "SHOW TABLES IN default;"
```

能列出 Iceberg 表名即正常。**直接 SELECT 会报错属正常现象**——Hive 3.1.3 没有
Iceberg StorageHandler（该集成包自 Iceberg 1.8.0 起停止发布，Hive 4.0 才内置）。
Iceberg 表由 Spark/Flink 读写，Hive 仅承担元数据仓库角色。

---

## 8. 常见问题（FAQ）

| 问题 | 原因与处理 |
|---|---|
| `ClassNotFoundException: HiveMetaStoreClient`（Spark） | 使用了不带 Hive 的 Spark 包；务必使用官方 `bin-hadoop3` 发行包（自带 Hive 2.3.10 客户端） |
| `ClassNotFoundException: HiveMetaStoreClient`（Flink） | 未按 5.3 节放置 Hive 依赖 jar |
| 连接 9083 超时 | hadoop102 的 metastore 未启动或防火墙未放行；`netstat -ntlp \| grep 9083` 确认 |
| HDFS Permission denied | `/warehouse/iceberg` 未授权；按第 6 节授权，或给提交用户单独赋权 |
| YARN 提交类冲突 | spark-env.sh 启用 `SPARK_DIST_CLASSPATH=$(hadoop classpath)`；Flink 确认 `HADOOP_CLASSPATH` 已导出 |
| Flink Hive 方言/HiveCatalog 报错 | `3.0.0-1.20` 连接器面向 Flink 1.20 构建，个别 Flink 侧 Hive 功能可能不兼容；Iceberg 功能不受影响，必要时切换方案 B |
| Java 17 反射告警 | Flink 2.1 默认已带所需 `--add-opens`；自定义 JVM 参数时保留原有 add-opens 列表 |
| CentOS 7 兼容性 | glibc 2.17 较老：Netty 自动回退 NIO（无害）；Flink 2.x 默认 Forst 状态后端若遇 native 库问题，可先切 Heap 状态后端验证；另 CentOS 7 已 EOL，建议中期升级 OS |
| Hive 里查 Iceberg 表报错 | 正常现象，见 7.4 节；不要在 Hive 侧对该表执行 DML |
| `UnsupportedClassVersionError`（61.0/52.0） | YARN 容器用了节点的 JDK 8 加载 Java 17 编译的 Spark 类；按 2.3 节给集群节点分发 JDK 17，并确认 spark-defaults.conf 已配 `spark.yarn.appMasterEnv.JAVA_HOME` 与 `spark.executorEnv.JAVA_HOME` |

---

## 9. 下载地址汇总

| 组件 | 地址 |
|---|---|
| Spark 4.1.3 | https://archive.apache.org/dist/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz |
| Flink 2.1.3 | https://archive.apache.org/dist/flink/flink-2.1.3/flink-2.1.3-bin-scala_2.12.tgz |
| Iceberg Spark runtime 1.11.0 | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar |
| Iceberg Flink runtime 1.11.0 | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar |
| Flink Hive SQL connector | https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/3.0.0-1.20/flink-sql-connector-hive-3.1.3_2.12-3.0.0-1.20.jar |
| Temurin JDK 17 | https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse |

---

## 10. 后续可选增强

- **V3 格式体验**：建表时 `TBLPROPERTIES ('format-version'='3')`，获得 Deletion Vector、行血缘（Row Lineage）、并发提交重试等新能力（1.11.0 已完整支持）
- **REST Catalog**：后续引入更多引擎或需要统一权限管控时，可部署 Iceberg REST Catalog 服务替代直连 HMS
- **Spark History Server**：`$SPARK_HOME/sbin/start-history-server.sh`，通过 http://hadoop105:18080 查看作业历史
- **Flink 生产模式**：建议 `yarn-application` 模式提交，并开启 checkpoint 到 HDFS

---

> 参考来源：[Iceberg Releases](https://iceberg.apache.org/releases/) · [Iceberg Flink 集成文档](https://iceberg.apache.org/docs/latest/flink/) · [Spark Hive Tables 文档](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html) · [Flink Hive 连接器文档](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/connectors/table/hive/overview/) · Maven Central 构件记录（均于 2026-07 核实）
