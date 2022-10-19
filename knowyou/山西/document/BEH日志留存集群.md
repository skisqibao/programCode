# BEH日志留存集群

## 一、相关组件WebUI信息

| 名称                   | URL访问地址                             | 物理地址IP   |
| ---------------------- | --------------------------------------- | ------------ |
| BEH集群管理平台        | http://10.231.32.13:8088/system         | 10.231.32.13 |
| Namenode Web UI        | https://za2301d04rs10hdp1502.sxmcc:9871 | 10.212.8.132 |
| Namenode Web UI        | https://za2301e03rs08hdp1646.sxmcc:9871 | 10.212.9.46  |
| ResourceManager Web UI | http://401b10rs07hdp050.sxmcc:23188     | 10.231.32.50 |
| ResourceManager Web UI | http://401c14rs03hdp059.sxmcc:23188     | 10.231.32.59 |
| HBase Hmaster Web UI   | http://401b18rs07hdp042.sxmcc:60010     | 10.231.32.42 |
| HBase Hmaster Web UI   | http://401b19rs07hdp051.sxmcc:60010     | 10.231.32.51 |
| Job History WebUI      | http://401b09rs06hdp049.sxmcc:19888     | 10.231.32.49 |
| Ranger Web UI          | http://10.231.32.59:6080                | 10.231.32.59 |
| Oozie Server WebUI     | http://401b18rs07hdp042.sxmcc:11000     | 10.231.32.42 |

## 二、Beeline连接串

```shell
beeline -u "jdbc:hive2://401a26rs07hdp023.sxmcc:2181,401b18rs07hdp042.sxmcc:2181,401b19rs07hdp051.sxmcc:2181,401b10rs07hdp050.sxmcc:2181,401c14rs03hdp059.sxmcc:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
```

## 三、Zookeeper地址

| 主机名                 | IP地址       | 端口 |
| ---------------------- | ------------ | ---- |
| 401a26rs07hdp023.sxmcc | 10.231.32.23 | 2181 |
| 401b18rs07hdp042.sxmcc | 10.231.32.42 | 2181 |
| 401b19rs07hdp051.sxmcc | 10.231.32.51 | 2181 |
| 401b10rs07hdp050.sxmcc | 10.231.32.50 | 2181 |
| 401c14rs03hdp059.sxmcc | 10.231.32.59 | 2181 |

**Zookeeper连接串：10.231.32.42:2181,10.231.32.51:2181,10.231.32.23:2181,10.231.32.50:2181,10.231.32.59:2181**

## 四、JobTracker地址

| 主机名                 | IP地址       | 端口 |
| ---------------------- | ------------ | ---- |
| 401b10rs07hdp050.sxmcc | 10.231.32.50 | 8032 |
| 401c14rs03hdp059.sxmcc | 10.231.32.59 | 8032 |

## 五、HDFS角色信息

| 角色名称 | 主机名                     | IP地址       | 端口 |
| -------- | -------------------------- | ------------ | ---- |
| NameNode | za2301d04rs10hdp1502.sxmcc | 10.212.8.132 | 9000 |
| NameNode | za2301e03rs08hdp1646.sxmcc | 10.212.9.46  | 9000 |

```shell
# HDFS联合域名
hdfs://beh001
```

## 六、Hive执行引擎说明

- 集群默认计算引擎使用：MapReduce

- 集群可支持Hive计算的引擎：MapReduce、Spark、Tez

- 推荐使用的计算引擎：Tez

  Tez执行引擎使用方式，在Beeline中设置：

  ```SQL
  set hive.execution.engine=tez;
  set hive.tez.container.size=1024;
  ```

