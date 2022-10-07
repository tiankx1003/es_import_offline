# lucene generate

Ubuntu 22.04.1 LTS x86_64
Memory: 4230MiB / 15902MiB
CPU: Intel i5-8250U (8) @ 3.400GHz

CentOS Linux 7 (Core) x86_64
Memory: 1926MiB / 3934MiB
CPU: AMD Ryzen 5 4600H with Radeon Graphics (1) @ 2.994GHz

jdk1.8.0_291
scala-2.12.17

hadoop-3.3.4 (Pseudo-Distributed)
hive-3.1.3
spark-3.3.0-bin-hadoop3

docker 1.13.1
mysql-5.6.17 (docker)
zookeeper-3.7.0 (docker)

```shell
docker run -d -p 3307:3306 -e MYSQL_ROOT_PASSWORD=root --name mysql-5.6.17 mysql:5.6.17
docker run -d -p 2181:2181 --name es_zk --restart always zookeeper
```
