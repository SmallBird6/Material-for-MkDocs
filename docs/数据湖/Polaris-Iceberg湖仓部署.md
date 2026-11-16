# Polaris + Iceberg 湖仓部署

## 一、组件版本

| 组件             | 版本                   | 说明                                           |
| -------------- | -------------------- | -------------------------------------------- |
| Apache Iceberg | 1.11.0               | 表格式，2026-05-19 发布                            |
| Apache Polaris | 1.6.0                | REST Catalog，2026-07-08 发布，基于 Quarkus 3.36.3 |
| Apache Spark   | 4.1.3                | 批处理引擎，Scala 2.13                             |
| Apache Flink   | 2.1.3                | 流处理引擎                                        |
| MinIO          | 2021-06-17T00:10:46Z | S3 兼容存储（旧版本）                                 |
| PostgreSQL     | -                    | Polaris 元数据持久化                               |
| JDK            | 21                   | 位于 /data/jdk21，不配置全局环境变量                     |

这套架构下，有一个问题， `AWS SDK 2.33.4 + MinIO 2021`需要设置WHEN_REQUIRED禁用新校验和

## 二、部署步骤

### 2.1 添加配置

1. 创建配置：`mkdir /data/polaris-1.6.0/config`
2. 创建配置文件：`vim /data/polaris-1.6.0/config/application.properties`
3. 配置内容：
```properties
# ============================================================
# Apache Polaris 1.6.0 配置
# ============================================================

# --- HTTP 端口 ---
quarkus.http.port=8181
quarkus.management.port=8182

# --- PostgreSQL 数据源 ---
quarkus.datasource.jdbc.url=jdbc:postgresql://172.16.15.15:5432/polaris
quarkus.datasource.username=postgres
quarkus.datasource.password=PostgresRoot@2026!
quarkus.datasource.db-kind=postgresql

# --- 持久化类型（必须为 jdbc，否则重启数据丢失）---
polaris.persistence.type=relational-jdbc

# --- 固定 bootstrap 凭证（避免每次重启生成新凭证）---
polaris.bootstrap.credentials=POLARIS,root,admin123456

# --- 特性开关 ---
polaris.feature-configuration.ALLOW_INSECURE_STORAGE_TYPES=true

# --- 日志 ---
quarkus.log.level=INFO
quarkus.log.category."org.apache.polaris".level=INFO
quarkus.log.file.path=/data/polaris-1.6.0/logs/polaris.log
```

### 2.2 初始化数据库

1. 创建数据库：`CREATE DATABASE polaris;`
2. 执行bootstrap
```bash
/data/jdk21/bin/java \
  -Dquarkus.config.locations=/data/polaris-1.6.0/config/application.properties \
  -jar /data/polaris-1.6.0/admin/quarkus-run.jar \
  bootstrap -r POLARIS -c 'POLARIS,root,admin123456' -p
```

### 2.3 启动Polaris

1. 创建启动脚本：`mkdir /data/polaris-1.6.0/sbin`
2. 编写启动脚本：`vim /data/polaris-1.6.0/sbin/start.sh`
```bash
#!/bin/bash
export JAVA_HOME=/data/jdk21
export PATH=$JAVA_HOME/bin:$PATH
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY='!Qivie20130113'
export AWS_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED
export AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED

exec $JAVA_HOME/bin/java \
  -Xms512m -Xmx2g \
  -Dquarkus.config.locations=/data/polaris-1.6.0/config/application.properties \
  -jar /data/polaris-1.6.0/server/quarkus-run.jar \
  "$@"
```
3. 执行权限：`chmod +x /data/polaris-1.6.0/sbin/start.sh`
4. 后台启动：`nohup /data/polaris-1.6.0/sbin/start.sh > /dev/null 2>&1 &`

### 2.4 Polaris检查

1. 健康检查：`curl -s http://localhost:8182/q/health`
2. 获取Token
```bash
curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user 'root:admin123456' \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL'
```

### 2.5 创建MinIO Catalog

1. 获取Token
```bash
export POLARIS_TOKEN=$(curl -s http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user 'root:admin123456' \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r .access_token)
```
2. 创建
```bash
curl -s -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "minio_catalog",
      "type": "INTERNAL",
      "readOnly": false,
      "properties": {
        "default-base-location": "s3://iceberg-warehouse/"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://iceberg-warehouse/"],
        "endpoint": "http://172.16.15.28:9000",
        "region": "us-east-1",
        "pathStyleAccess": true
      }
    }
  }'
```
3. 同步再Minio页面上创建bucket

## 三、Spark验证

### 3.1 所需Jar包

| JAR                                       | 下载地址                                                                                                                              |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| iceberg-spark-runtime-4.1_2.13-1.11.0.jar | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar |
| iceberg-aws-bundle-1.11.0.jar             | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar                         |
下载后放入jars目录

### 3.2 启动命令

```bash
/data/spark-4.1.3/bin/spark-sql \
  --master yarn \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.polaris.type=rest \
  --conf spark.sql.catalog.polaris.uri=http://localhost:8181/api/catalog \
  --conf spark.sql.catalog.polaris.warehouse=minio_catalog \
  --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.polaris.token-refresh-enabled=true \
  --conf spark.sql.catalog.polaris.credential=root:admin123456 \
  --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.polaris.s3.endpoint=http://172.16.15.28:9000 \
  --conf spark.sql.catalog.polaris.s3.path-style-access=true \
  --conf spark.sql.catalog.polaris.s3.access-key-id=admin \
  --conf spark.sql.catalog.polaris.s3.secret-access-key='!Qivie20130113' \
  --conf spark.sql.catalog.polaris.s3.checksum-enabled=false \
  --conf spark.sql.defaultCatalog=polaris \
  --conf spark.executor.extraJavaOptions="-Daws.requestChecksumCalculation=WHEN_REQUIRED -Daws.responseChecksumValidation=WHEN_REQUIRED" \
  --conf spark.driver.extraJavaOptions="-Daws.requestChecksumCalculation=WHEN_REQUIRED -Daws.responseChecksumValidation=WHEN_REQUIRED"
```

### 3.3 验证SQL

```sql
use polaris;
CREATE NAMESPACE db1;
USE db1;
CREATE TABLE spark_test (id INT, name STRING);
INSERT INTO spark_test VALUES (1, 'hello');
SELECT * FROM spark_test;
```

## 四、Flink验证

### 4.1 所需Jar包

| JAR                                  | 下载地址                                                                                                                    |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| iceberg-flink-runtime-2.1-1.11.0.jar | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar |
| iceberg-aws-bundle-1.11.0.jar        | https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar               |
下载后放入lib目录

### 4.2 添加配置

修改flink-env.sh

```bash
export AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED
export AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED
```

### 4.3 启动验证

1. `yarn-session.sh`
2. `sql-client.sh`
3. 进行验证，查询刚刚spark引擎创建的表

```sql
CREATE CATALOG polaris WITH (
  'type'                 = 'iceberg',
  'catalog-impl'         = 'org.apache.iceberg.rest.RESTCatalog',
  'uri'                  = 'http://localhost:8181/api/catalog',
  'warehouse'            = 'minio_catalog',
  'credential'           = 'root:admin123456',
  'scope'                = 'PRINCIPAL_ROLE:ALL',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://172.16.15.28:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = '!Qivie20130113',
  's3.checksum-enabled'  = 'false'
);

USE CATALOG polaris;

USE db1;

SET 'sql-client.execution.result-mode' = 'tableau';
SELECT * FROM flink_test;
```
