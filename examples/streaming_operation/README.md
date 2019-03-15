# CerebralCortex-kernel - Spark Streaming Example
CerebralCortex accepts data from mCerebrum, process files, store data, and publish newly stored file names on Kafka message queue.
In this example:
 - Pyspark-Kafka direct stream is created
 - Read parquet data and convert it into pandas dataframe
 - Gaussian noise is added in sample data
 - Noisy data is stored as a new stream
 - Noisy and clean data is retrieved and printed on console

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
- Open [http://localhost/api/v2/docs/](http://localhost/api/v2/docs/)
- Click on `user : Authentication service` to expand it.
    - Click on `POST /user/register` and register a user. Simply click on `ModelExample Value` box, it will copy json data in text box. Click on Try it out. This should register a user with `username=string` and `password=string`
    - Click on `POST /user/login` to get auth token. Simply click on `ModelExample Value` box, it will copy json data in text box. Click on Try it out. This should auth token under `Response Body`
- Click on `stream : Data and annotation streams` to expand it.
    - Click on `POST /stream/register` to register a stream. Simply click on `ModelExample Value` box, it will copy json data in text box. Click on Try it out. This should register a new stream with the name of `string`. Under `Response Body`, there will be `hash_id`. 
    - Click on `PUT /stream/{metadata_hash}` to upload data. Paste `hash_id` in the `metadata_hash` field and `auth token` in the field of `Authorization`. Browse and upload a sample data file located in `CerebralCortex-Kernel-Examples/raw_data/XXX.gz`. If everything goes fine, under `Response Body` a success message will appear.

    

## Installation

* `git clone https://github.com/MD2Korg/CerebralCortex-Kernel-Examples.git`
 
* `cd CerebralCortex-Kernel-Examples`

* `sudo pip3 install -r requirements.txt`

    - Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.) installed on your machine 

 
## How to run the example code?
    - `cd CerebralCortex-Kernel-Examples/examples/streaming_operation`
    - `sh run.sh`

If everything works well then example code will produce similar output on console as below:
