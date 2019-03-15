# CerebralCortex-kernel - Spark Streaming Example
CerebralCortex accepts data from mCerebrum, process files, store data, and publish newly stored file names on Kafka message queue.
In this example:
 - Example code makes call to CerebralCortex-APIServer to:
    - Authenticate a user
    - Register a new stream (`accelerometer--org.md2k.phonesensor--phone`)
    - Upload sample data
 - Pyspark-Kafka direct stream is created
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
        - `tar -xzf spark*.tar.gz`
    - Spark requires Java-8 installed

## Setup CerebralCortex
- Follow instructions to install/configure CerebralCortex ([Click here to view instructions](https://github.com/MD2Korg/CerebralCortex))

    

## Installation

* `git clone https://github.com/MD2Korg/CerebralCortex-Kernel-Examples.git`
 
* `cd CerebralCortex-Kernel-Examples`

* `sudo pip3 install -r requirements.txt`

    - Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.) installed on your machine 

 
## How to run the example code?
    - `cd CerebralCortex-Kernel-Examples/examples/streaming_operation`
    - `sh run.sh`

If everything works well then example code will produce similar output on console as below:

`TODO - Provide sample output`

