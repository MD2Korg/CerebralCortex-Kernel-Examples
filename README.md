# CerebralCortex-kernel (Examples)
This directory contains some of the following examples on how to get/save data streams and perform basic operations:

* Window stream data into 1 minute chunks

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
    - Edit `SPARK_HOME` in `CerebralCortex-Kernel-Examples/examples/basic_windowing/run.sh` to point to the location you extracted Spark to.
	    - ex: `export SPARK_HOME=~/spark-2.4.0-bin/hadoop2.7/` if Spark was extracted to your home directory.
	- `export PYTHONPATH="${PYTHONPATH}":PATH-OF-MAIN-DIR/CerebralCortex-Kernel-Examples` (This is defined in `CerebralCortex-Kernel-Examples/examples/basic_windowing/run.sh`)

* MySQL > 5.7
    - You might have to set up a MySQL user. 

## Installation

* `git clone https://github.com/MD2Korg/CerebralCortex-Kernel-Examples.git`
 
* `cd CerebralCortex-Kernel-Examples`

* `sudo pip3 install -r requirements.txt`

    - Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.) installed on your machine 

 
## Configure CerebralCortex-Kernel
* Update MySQL settings in `CerebralCortex-Kernel-Examples/conf/cerebralcortex.yml` file, for example, mysql username, password etc.. Please look at the comments on what params shall be updated.

## How to run the example code?
* **Import MySQL Database:**
    - `cd CerebralCortex-Kernel-Examples/resources/db`
    - `mysql -u MySQL-USERNAME -pMySQL-PASSWORD < cerebralcortex.sql `

* **Run example**    
    - `cd CerebralCortex-Kernel-Examples/examples/basic_windowing`
    - `sh run.sh`

If everything works well then example code will produce similar output on console as below:

``` 
 ********** STREAM VERSION **********
stream-version: 1


 ********** STREAM DATA **********
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:35:00 End-time: 2019-01-09 11:36:00 Average-battery-levels: 100.0
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:41:00 End-time: 2019-01-09 11:42:00 Average-battery-levels: 96.58333333333333
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:43:00 End-time: 2019-01-09 11:44:00 Average-battery-levels: 95.23333333333333
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:38:00 End-time: 2019-01-09 11:39:00 Average-battery-levels: 98.28333333333333
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:39:00 End-time: 2019-01-09 11:40:00 Average-battery-levels: 97.93333333333334


 ********** STORING NEW STREAM DATA **********
BATTERY--org.md2k.phonesensor--PHONE-windowed-data has been stored.
```
