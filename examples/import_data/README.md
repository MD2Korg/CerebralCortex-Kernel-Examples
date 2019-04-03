# CerebralCortex-kernel (Import Data Example)
This directory contains some of the following example on how to import data in CerebralCortex format.

## Dependencies
* [Python3.6](https://www.python.org/downloads/release/python-360/) 
    - Note: Python3.7 is not compatible with some of the requirements
    - Make sure pip version matches Python version 

* MySQL > 5.7 (distribution number)
    - You might have to set up a MySQL user. 
    - check your MySQL version 
        ```
          $ mysql --version
          mysql  Ver 14.14 Distrib 5.7.25, for Linux (x86_64) using  EditLine wrapper
          ```

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
    
    - Open main.py update parameters
    - `python main.py`
