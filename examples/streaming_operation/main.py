# Copyright (c) 2017, MD2K Center of Excellence
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
import numpy as np
import json
import warnings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def add_gaussian_noise(msg, cc_config_path):

    # Disable pandas warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)

    '''
    create CC object again. This code is running on worker node. Thus, it won't have access to CC object created in run()
    CC object cannot be passed to worker nodes because it contains sockets and sockets cannot be serialized in spark to pass as a parameter
    '''

    CC = Kernel(cc_config_path, enable_spark=False)
    stream_name = "phone_platform_annotation" #msg.get("stream_name") #TODO: get it from kafka msg
    user_id = "34b9a373-e0ed-3bec-bdd3-495095f2282c"#msg.get("user_id") #TODO: get it from kafka msg

    data = pq.read_table(msg.get("filename"))
    pdf = data.to_pandas()

    mu, sigma = 0, 0.1
    pdf_total_rows = pdf.shape[0]
    pdf_total_columns = pdf.shape[1]-2
    noise = np.random.normal(mu, sigma, [pdf_total_rows, pdf_total_columns])
    print(noise)

    #signal = clean_signal + noise

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
    records = rdd.map(lambda r: json.loads(r[1]))
    result = records.map(lambda msg: add_gaussian_noise(msg, cc_config_path))
    print("File Iteration count:", result.count())

def run():

    # create cerebralcortex object
    cc_config_path = "../../conf/"
    CC = Kernel(cc_config_path, enable_spark_ui=True)

    if CC.config["messaging_service"]=="none":
        raise Exception("Messaging service is disabled (none) in cerebralcortex.yml. Please update configs.")

    # Kafka Consumer Configs
    spark_context = get_or_create_sc(type="sparkContext")

    ssc = StreamingContext(spark_context, int(CC.config["kafka"]["ping_kafka"]))
    kafka_files_stream = CC.MessagingQueue.create_direct_kafka_stream("filequeue", ssc)
    if kafka_files_stream is not None:
        kafka_files_stream.foreachRDD(lambda rdd: iterate_on_rdd(rdd, cc_config_path))

    ssc.start()
    ssc.awaitTermination()




if __name__ == "__main__":
    run()
