# CerebralCortex-kernel - Spark Streaming Example
CerebralCortex accepts data from mCerebrum, process files, store data, and publish newly stored file names on Kafka message queue.
In this example:
 - Make call to CerebralCortex-APIServer to:
    - Authenticate a user
    - Register a new stream (`accelerometer--org.md2k.phonesensor--phone`)
    - Upload sample data
 - Create Pyspark-Kafka direct stream
 - Read parquet data and convert it into pandas dataframe
 - Add gaussian noise in sample data
 - Store noisy data as a new stream
 - Retrieve and print noisy/clean data streams

## Dependencies
* [Python3.6](https://www.python.org/downloads/release/python-360/) 
    - Note: Python3.7 is not compatible with some of the requirements
    - Make sure pip version matches Python version 
* spark 2.4
    - Download and extract [Spark 2.4](https://spark.apache.org/downloads.html)
        - `cd ~/`
        - `wget http://apache.claz.org/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz` 
        - `tar -xzf spark-2.4.0-bin-hadoop2.7.tgz`
    - Spark requires Java-8 installed

## Setup CerebralCortex
- Follow instructions to install/configure CerebralCortex ([Click here to view the instructions](https://github.com/MD2Korg/CerebralCortex))


## Installation

* `git clone https://github.com/MD2Korg/CerebralCortex-Kernel-Examples.git`
 
* `cd CerebralCortex-Kernel-Examples`

* `sudo pip3 install -r requirements.txt`

    - Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.) installed on your machine 

- Update the path  `conf/cerebralcortex.yml` and change `filesystem->filesystem_path` to:
```
filesystem:
  filesystem_path: PATH-OF-CEREBRALCORTEX/mount_points/apiserver/
```

*Note*:  `PATH-OF-CEREBRALCORTEX` is the path where you cloned CerebralCortex repo.

- update spark path in `CerebralCortex-Kernel-Examples/streaming_operation/run.sh`
    - `export SPARK_HOME=/MAIN-DIR-PATH/spark-2.4.0-bin-hadoop2.7`

## How to run the example code?
    - `cd CerebralCortex-Kernel-Examples/examples/streaming_operation`
    - `sh run.sh`

If everything works well then example code will produce similar output on console as below:

```
*************** CLEAN DATA ***************
+-----------------------+----------------+--------------------+------------------+------------------+-------+------------------------------------+
|timestamp              |localtime       |accelerometer_x     |accelerometer_y   |accelerometer_z   |version|user                                |
+-----------------------+----------------+--------------------+------------------+------------------+-------+------------------------------------+
|2019-03-18 12:40:15.341|1552930793741000|-0.04712959312882263|0.6377598318241908|0.7602050034642583|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.401|1552930793801000|-0.0427370528319317 |0.6299500071674312|0.7571532456517583|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.461|1552930793861000|-0.04529884952774592|0.6201881152045107|0.7620341916332186|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.521|1552930793921000|-0.03931976847206294|0.6012740606683231|0.7962023689355567|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.585|1552930793985000|-0.05323155332170616|0.6298286834623471|0.7572761247889589|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
+-----------------------+----------------+--------------------+------------------+------------------+-------+------------------------------------+
only showing top 5 rows

*************** NOISY DATA ***************
+-----------------------+----------------+---------------------+------------------+------------------+-------+------------------------------------+
|timestamp              |localtime       |accelerometer_x      |accelerometer_y   |accelerometer_z   |version|user                                |
+-----------------------+----------------+---------------------+------------------+------------------+-------+------------------------------------+
|2019-03-18 12:40:15.341|1552930793741000|-0.13368666246479502 |0.8369798025424512|0.6728056597706673|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.401|1552930793801000|-0.060348290009912986|0.6213506186518224|0.9612845134658468|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.461|1552930793861000|-0.11765282585530606 |0.7160324411511608|0.6821023463569255|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.521|1552930793921000|-0.17304255479750524 |0.470732015146409 |0.7715337662489906|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
|2019-03-18 12:40:15.585|1552930793985000|-0.16743168070950443 |0.7350478645121761|0.8672737497469011|1      |34b9a373-e0ed-3bec-bdd3-495095f2282c|
+-----------------------+----------------+---------------------+------------------+------------------+-------+------------------------------------+
only showing top 5 rows

```

