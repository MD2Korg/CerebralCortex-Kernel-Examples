# Copyright (c) 2019, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from cerebralcortex.core.util.spark_helper import get_or_create_sc
from pyspark.streaming import StreamingContext
from cerebralcortex.core.config_manager.config import Configuration
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.kernel import Kernel
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex_rest import client
import numpy as np
import json
import warnings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def add_gaussian_noise(pdf:pd.DataFrame):
    """
    Add gaussian noise to data

    Args:
        pdf (pd.DataFrame): pandas dataframe

    Returns:
        pd.DataFrame: pandas dataframe

    Notes:
        Privacy layer for data could be added here. For example, adding noise in data before storing.

    """
    mu, sigma = 0, 0.1
    pdf_total_rows = pdf.shape[0]
    pdf_total_columns = pdf.shape[1]-2
    noise = np.random.normal(mu, sigma, [pdf_total_rows, pdf_total_columns])

    pdf["accelerometer_x"] = pdf["accelerometer_x"]+noise[:,0]
    pdf["accelerometer_y"] = pdf["accelerometer_y"]+noise[:,1]
    pdf["accelerometer_z"] = pdf["accelerometer_z"]+noise[:,2]
    return pdf

def process_save_stream(msg:dict, cc_config_path:str):
    """
    Process one of kafka messages, add gaussian noise to data and store data as a new stream

    Args:
        msg (dict): kafka message - {'filename': str, 'metadata_hash': str, "stream_name": str, "user_id": str}
        cc_config_path (str): path of cerebralcortex configs

    Notes:
        This method creates CC object again. This code is running on worker node. Thus, it won't have access to CC object created in run()
        CC object cannot be passed to worker nodes because it contains sockets and sockets cannot be serialized in spark to pass as a parameter

    """

    # Disable pandas warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)

    CC = Kernel(cc_config_path, enable_spark=False)
    cc_config = CC.config
    stream_name = msg.get("stream_name")
    user_id = msg.get("user_id")


    if cc_config["nosql_storage"] == "filesystem":
        file_name = str(cc_config["filesystem"]["filesystem_path"])+msg.get("filename")
    elif cc_config["nosql_storage"] == "hdfs":
        file_name = str(cc_config["hdfs"]["raw_files_dir"])+msg.get("filename")
    else:
        raise Exception(str(cc_config["nosql_storage"]) + " is not supported. Please use filesystem or hdfs.")

    data = pq.read_table(file_name)
    pdf = data.to_pandas()

    pdf = add_gaussian_noise(pdf)

    new_stream_name = stream_name+"_gaussian_noise"

    metadata = Metadata().set_name(new_stream_name).set_description("Gaussian noise added to the accel sensor stream.") \
        .add_dataDescriptor(
        DataDescriptor().set_attribute("description", "noisy accel x")) \
        .add_dataDescriptor(
        DataDescriptor().set_attribute("description", "noisy accel y")) \
        .add_dataDescriptor(
        DataDescriptor().set_attribute("description", "noisy accel z")) \
        .add_module(
        ModuleMetadata().set_name("cerebralcortex.streaming_operation.main").set_version("0.0.1").set_attribute("description", "Spark streaming example using CerebralCortex. This example adds gaussian noise to a stream data.").set_author(
            "test_user", "test_user@test_email.com"))

    pdf["user"] = user_id
    ds = DataStream(data=pdf, metadata=metadata)
    CC.save_stream(ds)


def iterate_on_rdd(rdd, cc_config_path):
    """
    Iterate over an RDD and pass each kafka message in the RDD to process_save_stream method

    Args:
        rdd (RDD): pyspark RDD
        cc_config_path (str): path of cerebralcortex configs
    """
    records = rdd.map(lambda r: json.loads(r[1]))
    result = records.map(lambda msg: process_save_stream(msg, cc_config_path))
    print("File Iteration count:", result.count())

def upload_stream_data(base_url:str, username:str, password:str, stream_name:str, data_file_path:str):
    """
    Upload stream data to cerebralcortex storage using CC-ApiServer

    Args:
        base_url (str): base url of CerebralCortex-APIServer. For example, http://localhost/
        username (str): username
        password (str): password of the user
        data_file_path (str): stream data file path that needs to be uploaded

    Raises:
        Exception: if stream data upload fails

    """

    login_url = base_url+"api/v3/user/login"
    register_stream_url = base_url+"api/v3/stream/register"

    metadata = Metadata().set_name(stream_name).set_description("mobile phone accelerometer sensor data.") \
        .add_dataDescriptor(
        DataDescriptor().set_name("accelerometer_x").set_type("float").set_attribute("description", "acceleration minus gx on the x-axis")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("accelerometer_y").set_type("float").set_attribute("description", "acceleration minus gy on the y-axis")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("accelerometer_z").set_type("float").set_attribute("description", "acceleration minus gz on the z-axis")) \
        .add_module(
        ModuleMetadata().set_name("cerebralcortex.streaming_operation.main").set_version("2.0.7").set_attribute("description", "data is collected using mcerebrum.").set_author(
            "test_user", "test_user@test_email.com"))

    stream_metadata = metadata.to_json()

    auth = client.login_user(login_url, username, password)
    status = client.register_stream(register_stream_url, auth.get("auth_token"), stream_metadata)
    stream_upload_url = base_url+"api/v3/stream/"+status.get("hash_id")
    result = client.upload_stream_data(stream_upload_url, auth.get("auth_token"), data_file_path)
    print(result)

def run():
    """
    This example:
     - Make call to CerebralCortex-APIServer to:
        - Authenticate a user
        - Register a new stream (`accelerometer--org.md2k.phonesensor--phone`)
        - Upload sample data
     - Create Pyspark-Kafka direct stream
     - Read parquet data and convert it into pandas dataframe
     - Add gaussian noise in sample data
     - Store noisy data as a new stream
     - Retrieve and print noisy/clean data streams
    """

    # upload sample data and publish messages on Kafka
    #rest_api_client("http://0.0.0.0:8089/")

    # create cerebralcortex object
    cc_config_path = "../../conf/"
    CC = Kernel(cc_config_path, enable_spark_ui=True)
    sample_stream_name = "accelerometer--org.md2k.phonesensor--phone"

    upload_stream_data("http://localhost/", "string", "string",sample_stream_name, "../../resources/sample_data/msgpack_files/phone_accel.msgpack.gz")

    # raise Exception
    if CC.config["messaging_service"]=="none":
        raise Exception("Messaging service is disabled (none) in cerebralcortex.yml. Please update configs.")

    # Kafka Consumer Configs
    spark_context = get_or_create_sc(type="sparkContext")

    ssc = StreamingContext(spark_context, int(CC.config["kafka"]["ping_kafka"]))
    kafka_files_stream = CC.MessagingQueue.create_direct_kafka_stream("filequeue", ssc)
    if kafka_files_stream is not None:
        kafka_files_stream.foreachRDD(lambda rdd: iterate_on_rdd(rdd, cc_config_path))

    ssc.start()
    ssc.awaitTermination(timeout=7)
    ssc.stop()

    CC = Kernel(cc_config_path, enable_spark_ui=True)
    print("*"*15,"CLEAN DATA","*"*15)
    ds_clean = CC.get_stream(stream_name=sample_stream_name)
    ds_clean.show(5, truncate=False)

    print("*"*15,"NOISY DATA","*"*15)
    ds_noise = CC.get_stream(stream_name=sample_stream_name+"_gaussian_noise")
    ds_noise.show(5, truncate=False)



if __name__ == "__main__":
    run()
