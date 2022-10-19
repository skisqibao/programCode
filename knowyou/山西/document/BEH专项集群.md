# BEH专项集群

## 一、相关组件WebUI信息

| 名称                   | URL访问地址                       | 物理地址IP    |
| ---------------------- | --------------------------------- | ------------- |
| BEH集群管理平台        | http://10.231.32.53:8088/system   | 10.231.32.53  |
| Namenode Web UI        | https://za2301g06rs07.sxmcc:9871  | 10.213.92.37  |
| Namenode Web UI        | https://za2301g07rs08.sxmcc:9871  | 10.213.92.47  |
| ResourceManager Web UI | https://za2301g12rs01.sxmcc:23188 | 10.213.92.59  |
| ResourceManager Web UI | https://za2301h02rs01.sxmcc:23188 | 10.213.92.113 |
| HBase Hmaster Web UI   | http://za2301g12rs01.sxmcc:60010  | 10.213.92.59  |
| HBase Hmaster Web UI   | http://za2301h02rs01.sxmcc:60010  | 10.213.92.113 |
| HBase Hmaster Web UI   | http://za2301h06rs01.sxmcc:60010  | 10.213.92.141 |
| Job History WebUI      | http://za2301h06rs01.sxmcc:19888  | 10.213.92.141 |
| Ranger Web UI          | http://10.213.92.59:6080          | 10.213.92.59  |
| Oozie Server WebUI     | http://za2301g12rs01.sxmcc:11000  | 10.213.92.59  |

## 二、Beeline连接串

```shell
beeline -u "jdbc:hive2://za2301g12rs01.sxmcc:2181,za2301h02rs01.sxmcc:2181,za2301h06rs01.sxmcc:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
-- 设置Yarn队列
set mapreduce.job.queuename=xxx;
```

## 三、Zookeeper地址

| 主机名              | IP地址        | 端口 |
| ------------------- | ------------- | ---- |
| za2301g12rs01.sxmcc | 10.213.92.59  | 2181 |
| za2301h02rs01.sxmcc | 10.213.92.113 | 2181 |
| za2301h06rs01.sxmcc | 10.213.92.141 | 2181 |

**Zookeeper连接串：10.213.92.59:2181,10.213.92.113:2181,10.213.92.141:2181**

## 四、JobTracker地址

| 主机名              | IP地址        | 端口 |
| ------------------- | ------------- | ---- |
| za2301g12rs01.sxmcc | 10.213.92.59  | 8032 |
| za2301h02rs01.sxmcc | 10.213.92.113 | 8032 |

## 五、HDFS角色信息

| 角色名称 | 主机名              | IP地址       | 端口 |
| -------- | ------------------- | ------------ | ---- |
| NameNode | za2301g06rs07.sxmcc | 10.213.92.37 | 9000 |
| NameNode | za2301g07rs08.sxmcc | 10.213.92.47 | 9000 |

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

