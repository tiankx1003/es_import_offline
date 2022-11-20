dt=20991231
index=es_import_offline_20991231
type=datum
alias=es_import_offline
pk=pri_key
es=pri_key,value01,value02
hive=pri_key,value01,value02
#zip -d /opt/files/lucene_generate-1.1.20221002_alpha.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
export HADOOP_USER_NAME=tiankx
spark-submit \
    --driver-memory=1g \
    --master yarn \
    --deploy-mode=cluster \
    --queue default \
    --executor-memory=1g \
    --conf 'spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/mnt/es_data/es -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+UseAdaptiveSizePolicy -XX:ParallelGCThreads=8 -XX:+PrintGCTaskTimeStamps' \
    --conf spark.executor.memoryOverhead=1024 \
    --conf spark.memory.fraction=0.01 \
    --conf spark.executor.heartbeatInterval=60000 \
    --conf spark.network.timeout=120000 \
    --conf spark.hadoop.mapreduce.input.fileinputformat.split.minsize=1048576 \
    --conf spark.driver.userClassPathFirst=true \
    --class io.github.tiankx1003.LuceneGen \
    /opt/files/lucene_generate-1.1.20221002_alpha.jar \
    --zookeeper server01,server02,server03 \
    --chroot /es_import_offline \
    --hive-table tmp.es_import_offline \
    --where dt=$dt \
    --number-of-shards 9 \
    --repartition false \
    --partition-multiples 9 \
    --index-name ${index} \
    --type-name $type \
    --alias $alias \
    --id $pk \
    --routing $pk \
    --hdfs-work-dir /tmp/es_lucene_file \
    --local-data-dir /mnt/es_lucene_file \
    --bulk-actions=1000 \
    --bulk-size=150 \
    --bulk-flush-interval=15 \
    --index-es-fields=$es \
    --index-hive-fields=$hive