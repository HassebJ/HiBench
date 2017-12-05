#!/bin/bash
#set -o xtrace
# step 0. Runtime parameters: $1 = testname [groupby|sortby|terasort] and $2 = slurm_job_id
# step 1. Update the config dirs: JAVA_HOME, HADOOP_HOME, SPARK_HOME, CONF_BASE to <shankard-svn-checked-out>/take_spark_release_numbers/spark-basic
# step 2. Update data size and number of tasks based on your current test => MAPS, REDUCES, NUM_KVS for groupby/sortby 
#  TERASORT_DATASIZE for terasort
# step 3. Update hdfs-site.xml dfs.datanode.data.dir if needed only currently it is ramdisk, ssd hybrid 
# step 4. Update spark-defaults.conf if you want to add some new parameters like slab size
# TRY RUNNING IT!!!!!

# CHANGE HERE : cluster specific
#--------------------------------
node_name_suffix=""
WORKER_MEM_SPARK="96g"
DAEMON_MEM_SPARK="2g"
SSD_PATH_BASE=/scratch/javed.19/hadoop
#--------------------------------

export MYUSER=javed.19
if [ x$JAVA_HOME == x"" ]
then
    echo "Setting JAVA_HOME!"
    export JAVA_HOME=/home/luxi/util/jdk1.7.0
fi
export WORKDIR=`pwd`
#BENCHMARK_HOME=$(readlink -f $WORKDIR/../../HiBench)
BENCHMARK_HOME=~/HiBench

export HADOOP_HOME=/home/javed.19/HiBench/hadoop-2.7.4
#export HADOOP_HOME=/home/javed.19/rdma-hadoop-2.x-1.1.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
CONF_BASE=$BENCHMARK_HOME/sbatch_spark_scripts/spark_release_confs
CONF_SPARK=$CONF_BASE/spark
CONF_HADOOP=$CONF_BASE/hadoop
CONF_HIBENCH=$CONF_BASE/HiBench
IB_ENABLED=true

if [ ! -f $HOME/myhostnames ]
then
    echo "Please create $HOME/myhostnames!"
    #scontrol show hostnames > $HOME/myhostnames
    exit 1
fi
WORKER_CORES_SPARK=`grep "core id" /proc/cpuinfo | wc -l`

RAMDISK_PATH_HADOOP=/dev/shm/$MYUSER/hadoop/
HADOOP_NAME_DIR=/dev/shm/$MYUSER/hadoopNamenode
#if [[ $HOSTNAME == *storage* ]]
#then
SSD_PATH_HADOOP=/tmp/$MYUSER/hadoop
#SSD_PATH_HADOOP=/scratch/javed.19/hadoop
LUSTRE_PATH_DATA_DIR=/scratch/javed.19/hadoop
SSD_PATH_SPARK=/data/ssd1/$MYUSER/spark/
#else
#  SSD_PATH_HADOOP=$SSD_PATH_BASE/$MYUSER/hadoop/
#  SSD_PATH_SPARK=$SSD_PATH_BASE/$MYUSER/spark/
#fi

config_hadoop (){
    ib_enabled=$1
    if [ "$ib_enabled" == "y" ];
    then
        node_name_suffix="-ib"
    else
        node_name_suffix=""
    fi
    rm $HADOOP_HOME/etc/hadoop/slaves
    for ip_addr in `cat ~/myhostnames | sed -n "6,9 p"`; do
        echo $ip_addr$node_name_suffix >> $HADOOP_HOME/etc/hadoop/slaves
    done

    orig_master=`cat $HOME/myhostnames | head -1`
    master=$orig_master"$node_name_suffix"

    echo "got the master's hostname: $master"




    # start hadoop/hdfs/yarn
    #for i in `cat $HOME/myhostnames`; do killall -9 java; done
    cp $CONF_HADOOP/* $HADOOP_HOME/etc/hadoop/
    cp $CONF_HIBENCH/* $BENCHMARK_HOME/conf/
  	 sed -i "s|MASTER_REPLACE|$master|g" $BENCHMARK_HOME/conf/hadoop.conf
	sed -i "s|HADOOP_HOME_REPLACE|$HADOOP_HOME|g" $BENCHMARK_HOME/conf/hadoop.conf

  sed -i "s|IB_ENABLED_REPLACE|$IB_ENABLED|g" $HADOOP_HOME/etc/hadoop/*.xml
    sed -i "s|MASTER_REPLACE|$master|g" $HADOOP_HOME/etc/hadoop/*.xml
    sed -i "s|MASTER_REPLACE|$master|g" $HADOOP_HOME/conf/hadoop.conf
  #  sed -i "s|IB_ENABLED_REPLACE|$IB_ENABLED|g" $HADOOP_HOME/etc/hadoop/core-site.xml
    sed -i "s|JAVA_HOME_REPLACE|$JAVA_HOME|g" $HADOOP_HOME/etc/hadoop/hadoop-env.sh
    sed -i "s|HADOOP_HOME_REPLACE|$HADOOP_HOME|g" $HADOOP_HOME/etc/hadoop/*
  #  sed -i "s|HADOOP_HOME_REPLACE|$HADOOP_HOME|g" $HADOOP_HOME/conf/*.conf
  #  sed -i "s|IB_ENABLED_REPLACE|$IB_ENABLED|g" $HADOOP_HOME/etc/hadoop/hdfs-site.xml
    sed -i "s|MASTER_DFS_REPLACE|$orig_master|g" $HADOOP_HOME/etc/hadoop/hdfs-site.xml
    sed -i "s|SSD_HADOOP_REPLACE|$SSD_PATH_HADOOP|g" $HADOOP_HOME/etc/hadoop/*.xml
    sed -i "s|HADOOP_NAME_DIR_REPLACE|$HADOOP_NAME_DIR|g" $HADOOP_HOME/etc/hadoop/hdfs-site.xml
    sed -i "s|RAM_HADOOP_REPLACE|$RAMDISK_PATH_HADOOP|g" $HADOOP_HOME/etc/hadoop/hdfs-site.xml
   # sed -i "s|LUSTRE_PATH_DATA_DIR_REPLACE|$LUSTRE_PATH_DATA_DIR|g" $HADOOP_HOME/etc/hadoop/*.xml
    
    #cat $HOME/myhostnames | awk '{print $1"'"$node_name_suffix"'"}' | grep -v $master > $HADOOP_HOME/etc/hadoop/slaves
    cat $HADOOP_HOME/etc/hadoop/slaves
}

start_hadoop (){
    $HADOOP_HOME/sbin/stop-all.sh
    echo "Stopped..................."
    for ip_addr in `cat $HADOOP_HOME/etc/hadoop/slaves`; do
        ssh $ip_addr killall -9 java
        ssh $ip_addr rm -rf /tmp/$MYUSER/hadoop/*
        ssh $ip_addr rm -rf /dev/shm/$MYUSER/*
        ssh $ip_addr rm -rf /fusion/$MYUSER/hadoop/*
        ssh $ip_addr rm -rf $SSD_PATH_HADOOP/*
        ssh $ip_addr rm -rf $SSD_PATH_SPARK/*
        echo $ip_addr
    done
    #rm -rf /oasis/scratch/comet/$MYUSER/temp_project/sparklogs/*
    rm -rf $HADOOP_NAME_DIR

    #if [[ $TESTNAME == "groupby" || $TESTNAME == "sortby" ]] 
    #then
    #  echo "Skip to start hadoop...."
    #else
    $HADOOP_HOME/bin/hadoop namenode -format
    echo "Starting..................."
    $HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
    $HADOOP_HOME/sbin/hadoop-daemons.sh start datanode
    echo ".........Done HDFS/Hadoop Setup..........."
    $HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
    $HADOOP_HOME/sbin/yarn-daemons.sh start nodemanager
    echo ".........Done YARN Setup..........."
    #fi


}

run() {
    OPERATION=$1
    shift
    if [ "" = "$OPERATION" ];
    then
        config_hadoop
        start_hadoop
    elif [ "config" = "$OPERATION" ];
    then
        config_hadoop
    fi
}

run "$@"

not_needed () {


    # start spark built
    cp $CONF_SPARK/* $SPARK_HOME/conf/
    sed -i "s|MASTER_REPLACE|$master|g" $SPARK_HOME/conf/*
    sed -i "s|JAVA_HOME_REPLACE|$JAVA_HOME|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|SPARK_HOME_REPLACE|$SPARK_HOME|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|SPARK_HOME_REPLACE|$SPARK_HOME|g" $SPARK_HOME/conf/spark-defaults.conf
    sed -i "s|HADOOP_HOME_REPLACE|$HADOOP_HOME|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|SSD_SPARK_REPLACE|$SSD_PATH_SPARK|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|LOCAL_IP_SUFFIX_REPLACE|$node_name_suffix|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|WORKER_MEM_REPLACE|$WORKER_MEM_SPARK|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|DAEMON_MEMORY_REPLACE|$DAEMON_MEM_SPARK|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|WORKER_MEM_REPLACE|$WORKER_MEM_SPARK|g" $SPARK_HOME/conf/spark-defaults.conf
    sed -i "s|WORKER_CORE_REPLACE|$WORKER_CORES_SPARK|g" $SPARK_HOME/conf/spark-env.sh
    sed -i "s|SPARK_IB_ENABLED_REPLACE|$IB_ENABLED|g" $SPARK_HOME/conf/spark-defaults.conf
    sed -i "s|HADOOP_IB_ENABLED_REPLACE|$IB_ENABLED|g" $SPARK_HOME/conf/spark-defaults.conf
    echo "spark.executor.extraLibraryPath "$SPARK_HOME"/lib/native/Linux-amd64-64:$HADOOP_HOME/lib/native" >> $SPARK_HOME/conf/spark-defaults.conf
    echo "spark.driver.extraLibraryPath "$SPARK_HOME"/lib/native/Linux-amd64-64:$HADOOP_HOME/lib/native" >> $SPARK_HOME/conf/spark-defaults.conf
    if [[ $HOSTNAME == *storage* ]]; then echo "spark.shuffle.rdma.device.num 1" >> $SPARK_HOME/conf/spark-defaults.conf ;fi
    echo "spark.shuffle.rdma.memory.slab.size `expr 256 \* 1024 \* 1024`" >> $SPARK_HOME/conf/spark-defaults.conf
    #echo "spark.shuffle.rdma.chunk.size `expr 1024 \* 1024`" >> $SPARK_HOME/conf/spark-defaults.conf

    # replace dirs
    cp $HADOOP_HOME/etc/hadoop/slaves $SPARK_HOME/conf/slaves
    $SPARK_HOME/sbin/start-all.sh
    jps

    echo "+++++++++++++spark-env.sh+++++++++++++++++"
    cat $SPARK_HOME/conf/spark-env.sh
    echo "+++++++++++++spark-defaults.conf+++++++++++++++++"
    cat $SPARK_HOME/conf/spark-defaults.conf

    NUM_EXECUTORS=`wc -l $SPARK_HOME/conf/slaves | awk '{print $1}'`

    for i in `seq 1 $ITERATIONS`
    do
        sleep 10
        echo "++++++++++++++$TESTNAME runs in $i times++++++++++++++++"
        if [[ $TESTNAME == "groupby" ]]
        then
            #time $SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.GroupByTest --master spark://$master:7077 --num-executors $NUM_EXECUTORS --executor-cores $WORKER_CORES_SPARK  $SPARK_HOME/lib/spark-examples-*-hadoop*.jar $MAPS $NUM_KVS $KVSIZE $REDUCES
            time $SPARK_HOME/bin/spark-submit --class edu.osu.hibd.ohb.spark.GroupByTest --master spark://$master:7077 --num-executors $NUM_EXECUTORS --executor-cores $WORKER_CORES_SPARK $CONF_SPARK/ohb-spark-1.0_spark1.5.jar $MAPS $NUM_KVS $KVSIZE $REDUCES
        elif [[ $TESTNAME == "sortby" ]]
        then
            time $SPARK_HOME/bin/spark-submit --class edu.osu.hibd.ohb.spark.SortByTest --master spark://$master:7077 --num-executors $NUM_EXECUTORS --executor-cores $WORKER_CORES_SPARK $CONF_SPARK/ohb-spark-1.0.jar $MAPS $NUM_KVS $KVSIZE $REDUCES
        elif  [[ $TESTNAME == "terasort" ]]
        then
            $SPARK_HOME/bin/spark-submit --master spark://$master:7077 --num-executors $NUM_EXECUTORS --executor-cores $WORKER_CORES_SPARK --class com.github.ehiggs.spark.terasort.TeraGen $CONF_SPARK/spark-terasort-1.0-jar-with-dependencies.jar $TERASORT_DATASIZE hdfs://$master:9000/tera_in_$i
            sleep 10
            time $SPARK_HOME/bin/spark-submit --class com.github.ehiggs.spark.terasort.TeraSort --master spark://$master:7077 --num-executors $NUM_EXECUTORS --executor-cores $WORKER_CORES_SPARK $CONF_SPARK/spark-terasort-1.0-jar-with-dependencies.jar hdfs://$master:9000/tera_in_$i/ hdfs://$master:9000/tera_out_$i
        else
            echo "Unsupported Test!"
        fi
    done
}

