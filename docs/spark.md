---
title: This will be used as the title-tag of the page head
---

# Install and configure Spark and Hadoop

### 1. On a single server

**0. preparation**

Suppose the workstation has six spare hard drives, 16 CPU cores, and 64 GB memory. The following is observed after formating and mounting the hard drives separately:

```shell
dolores@workstation: ~$ df -Th
Filesystem     Type      Size  Used Avail Use% Mounted on
/dev/sda1      ext4      459G  114G  345G  25% /mnt/hadoop/disk1
/dev/sdb1      ext4      459G  113G  346G  25% /mnt/hadoop/disk2
/dev/sdc1      ext4      459G  113G  347G  25% /mnt/hadoop/disk3
/dev/sdd1      ext4      459G  112G  347G  25% /mnt/hadoop/disk4
/dev/sde1      ext4      459G  114G  346G  25% /mnt/hadoop/disk5
/dev/sdf1      ext4      459G  113G  346G  25% /mnt/hadoop/disk6
...
```

Make sure you can ssh to the  `localhost` without typing the password. If you cannot, set it up as follows:

```shell
dolores@workstation: ~$ ssh-keygen
dolores@workstation: ~$ cat ~/.ssh/id_rsa.pub >> ~/.authorized_keys
dolores@workstation: ~$ chmod 600 ~/.authorized_keys
dolores@workstation: ~$ ssh localhost
dolores@workstation: ~$ exit
```

Download the suitable version of JDK for your system [here](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), and then install it:

```shell
dolores@workstation: ~$ tar xf jdk-8u121-linux-x64.tar.gz
dolores@workstation: ~$ sudo cp -r jdk1.8.0_121 /opt/jdk1.8.0_121
```

**1. install and configure Hadoop**

Download and install:

```shell
dolores@workstation: ~$ wget http://mirror.olnevhost.net/pub/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
dolores@workstation: ~$ tar xf hadoop-2.6.5.tar.gz
dolores@workstation: ~$ sudo mv hadoop-2.6.5 /opt
```

Configure Hadoop:

```shell
dolores@workstation: ~$ cd /opt/hadoop-2.6.5/
dolores@workstation: hadoop-2.6.5$ mkdir data
dolores@workstation: cd etc/hadoop/
```

​	Edit the following files that are distributed with Hadoop as follows:

- `hadoop-env.sh`: 

   change the line `export JAVA_HOME=${JAVA_HOME}` to `export JAVA_HOME=/opt/jdk1.8.0_121`

- `core-site.xml`:

  ```xml
   <configuration>
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://localhost:9000</value>
       </property>
   </configuration>
  ```

- `hdfs-site.xml`:

  ```xml
   <configuration>
   	<property>
           <name>dfs.replication</name>
           <value>2</value>
       </property>
     	<property>
     		<name>dfs.namenode.name.dir</name>
     		<value>file:///opt/hadoop-2.6.5/data/namenode</value>
     	</property>
   	<property>
           <name>dfs.datanode.data.dir</name>     				<value>file:///mnt/hadoop/disk1,file:///mnt/hadoop/disk2,file:///mnt/hadoop/disk3,file:///mnt/hadoop/disk4,file:///mnt/hadoop/disk5,file:///mnt/hadoop/disk6</value>
       </property>
   	<property>
           <name>dfs.blocksize</name>
           <value>33554432</value>
       </property>
   </configuration>
  ```

Format HDFS and start the service:

```shell
dolores@workstation: hadoop$ cd ../..
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs namenode -format
dolores@workstation: hadoop-2.6.5$ ./sbin/start-dfs.sh
```

Check if it is working properly:

```shell
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -mkdir -p /user/dolores
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -put etc/hadoop/core-site.xml
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -cat core-site.xml
dolores@workstation: hadoop-2.6.5$ ./bin/hdfs dfs -rm core-site.xml
```

Add the following to the file `~/.profile`  and re-login:

```shell
export PATH=/opt/hadoop-2.6.5/bin:$PATH
```

**2. install and configure Spark**

Download and install:

```shell
dolores@workstation: ~$ wget http://mirror.cogentco.com/pub/apache/spark/spark-2.1.0/spark-2.1.0-bin-without-hadoop.tgz
dolores@workstation: ~$ tar xf spark-2.1.0-bin-without-hadoop.tgz
dolores@workstation: ~$ sudo mv spark-2.1.0-bin-without-hadoop /opt/spark-2.1.0
```

Configure Spark:

```shell
dolores@workstation: ~$ cd /opt/spark-2.1.0/conf
dolores@workstation: conf$ cp spark-env.sh.template spark-env.sh
dolores@workstation: conf$ cp spark-defaults.conf.template spark-defaults.conf
dolores@workstation: conf$ cp log4j.properties.template log4j.properties
```

​	Edit the following files that are distributed with Spark as follows:

- `spark-env.sh`:

  ```shell
   export HADOOP_HOME=/opt/hadoop-2.6.5
   export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   source ${HADOOP_CONF_DIR}/hadoop-env.sh
   export SPARK_DIST_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)
   export SPARK_WORKDER_MEMORY=52g
   export SPARK_WORKER_CORES=16
   export SPARK_MASTER_HOST=localhost
   export SPARK_LOCAL_IP=127.0.0.1
  ```

   Note: set `$SPARK_WORKER_MEMORY` and `$SPARK_WORKER_CORES` to a reasonable value for your server.

- `spark-default.conf`:

  ```json
   spark.master							spark://localhost:7077
   spark.executor.memory			 	 	 52g
   spark.driver.memory               	  	  8g
   spark.driver.maxResultSize			 	 6g
   spark.serializer						org.apache.spark.serializer.KryoSerializer
   spark.rdd.compress						true
   spark.kryoserializer.buffer.max		 1024m
   spark.sql.warehouse.dir				/user/dolores
  ```

   Note: set the memory usage options to reasonable values for your server.

- `log4j.properties`:

   add one line `log4j.logger.org.apache.spark=WARN`

Start Spark service:

```shell
dolores@workstation: conf$ cd /opt/spark-2.1.0
dolores@workstation: spark-2.1.0$ ./sbin/start-all.sh
```

Check if it is working properly:

```shell
dolores@workstation: spark-2.1.0$ ./bin/spark-shell
```

Add the following to the file `~/.profile` and re-login:

```shell
export PATH=/opt/spark-2.1.0/bin:$PATH
```

### 2. On a cluster

**0. preparation**

Suppose the cluster to be set-up consist of 1 master server `node0` and 8 slave servers `node[1-8]`. Each slave server has 3 spare hard drives, 16 CPU cores, 64 GB memory. Format and mount the hard drives separately. The following is observed for each slave server:

```shell
dolores@node1: ~$ df -Th
Filesystem     Type      Size  Used Avail Use% Mounted on
/dev/sda1      ext4      459G  114G  345G  25% /mnt/hadoop/disk1
/dev/sdb1      ext4      459G  113G  346G  25% /mnt/hadoop/disk2
/dev/sdc1      ext4      459G  113G  347G  25% /mnt/hadoop/disk3
```

Make sure you can ssh to any of the nodes without typing the password.

Download the suitable version of JDK for your system [here](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), and then install it for all nodes as follows:

```shell
dolores@node0: ~$ tar xf jdk-8u121-linux-x64.tar.gz
dolores@node0: ~$ sudo cp -r jdk1.8.0_121 /opt/jdk1.8.0_121
```

**1. install and configure Hadoop**

For all nodes, download and install:

```shell
dolores@node0: ~$ wget http://mirror.olnevhost.net/pub/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz
dolores@node0: ~$ tar xf hadoop-2.6.5.tar.gz
dolores@node0: ~$ sudo mv hadoop-2.6.5 /opt
```

Configure Hadoop:

```shell
dolores@node0: ~$ cd /opt/hadoop-2.6.5/
dolores@node0: hadoop-2.6.5$ mkdir data
dolores@node0: cd etc/hadoop/
```

​       For all nodes, edit the following files that are distributed with Hadoop as follows: 

- `hadoop-env.sh`: 

  change the line `export JAVA_HOME=${JAVA_HOME}` to `export JAVA_HOME=/opt/jdk1.8.0_121`

- `core-site.xml`:

  ```xml
   <configuration>
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://node0:9000</value>
       </property>
     	<property>
   		<name>io.file.buffer.size</name>
   		<value>131072</value>
   	</property>
   </configuration>
  ```

   For `node0`, edit the following files:

- `hdfs-site.xml`:

  ```xml
   <configuration>
   	<property>
           <name>dfs.replication</name>
           <value>2</value>
       </property>
     	<property>
     		<name>dfs.namenode.name.dir</name>
     		<value>file:///opt/hadoop-2.6.5/data/namenode</value>
     	</property>
   	<property>
           <name>dfs.blocksize</name>
           <value>33554432</value>
       </property>
   </configuration>
  ```


- `slaves`:

  ```shell
  node1
  node2
  node3
  node4
  node5
  node6
  node7
  node8
  ```

  For `node[1-8]`, edit the following file:

- `hdfs-site.xml`

  ```xml
  <configuration>
  	<property>
          <name>dfs.datanode.data.dir</name>	
       <value>file:///mnt/hadoop/disk1,file:///mnt/hadoop/disk2,file:///mnt/hadoop/disk3</value>
      </property>
  </configuration>
  ```

Format HDFS and start the service:

```shell
dolores@node0: hadoop$ cd ../..
dolores@node0: hadoop-2.6.5$ ./bin/hdfs namenode -format
dolores@node0: hadoop-2.6.5$ ./sbin/start-dfs.sh
```

Check if it is working properly:

```shell
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -mkdir -p /user/dolores
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -put etc/hadoop/core-site.xml
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -cat core-site.xml
dolores@node0: hadoop-2.6.5$ ./bin/hdfs dfs -rm core-site.xml
```

Add the following to the file `~/.profile`  and re-login:

```shell
export PATH=/opt/hadoop-2.6.5/bin:$PATH
```

**2. install and configure Spark**

For all nodes, Download and install:

```shell
dolores@node0: ~$ wget http://mirror.cogentco.com/pub/apache/spark/spark-2.1.0/spark-2.1.0-bin-without-hadoop.tgz
dolores@node0: ~$ tar xf spark-2.1.0-bin-without-hadoop.tgz
dolores@node0: ~$ sudo mv spark-2.1.0-bin-without-hadoop /opt/spark-2.1.0
```

Configure Spark:

```shell
dolores@node0: ~$ cd /opt/spark-2.1.0/conf
dolores@node0: conf$ cp spark-env.sh.template spark-env.sh
dolores@node0: conf$ cp spark-defaults.conf.template spark-defaults.conf
dolores@node0: conf$ cp log4j.properties.template log4j.properties
```

​	For all nodes, edit the following files:

- `spark-env.sh`:

  ```shell
   export HADOOP_HOME=/opt/hadoop-2.6.5
   export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   source ${HADOOP_CONF_DIR}/hadoop-env.sh
   export SPARK_DIST_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)
   export SPARK_WORKER_CORES=16
   export SPARK_WORKDER_MEMORY=52g
  ```

   Note: set the `$SPARK_WORKER_MEMORY` to a reasonable value for your server.

- `spark-default.conf`:

  ```json
   spark.master							spark://node0:7077
   spark.executor.memory			 	 	 52g
   spark.driver.memory               	  	  8g
   spark.driver.maxResultSize			 	 6g
   spark.serializer						org.apache.spark.serializer.KryoSerializer
   spark.rdd.compress						true
   spark.kryoserializer.buffer.max	      1024m
   spark.sql.warehouse.dir				 /user/dolores
  ```

   Note: set the memory usage options to reasonable values for your server.

  For `node0`, edit the following files:

- `log4j.properties`:

    add one line    `log4j.logger.org.apache.spark=WARN`

- `slaves`:

  ```shell
  node1
  node2
  node3
  node4
  node5
  node6
  node7
  node8
  ```

Start Spark service:

```shell
dolores@node0: conf$ cd /opt/spark-2.1.0
dolores@node0: spark-2.1.0$ ./sbin/start-all.sh
```

Check if it is working properly:

```shell
dolores@node0: spark-2.1.0$ ./bin/spark-shell
```

Add the following to the file `~/.profile` and re-login:

```shell
export PATH=/opt/spark-2.1.0/bin:$PATH
```
