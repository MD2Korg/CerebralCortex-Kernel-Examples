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
from cerebralcortex.kernel import Kernel
import numpy as np
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def add_gaussian_noise(msg):

    msg = msg[1] # TODO: msg should not be a tuple
    if isinstance(msg, str):
        msg = json.loads(msg)

    data = pq.read_table(msg.get("filename"))
    df = data.to_pandas()

    mu, sigma = 0, 0.1
    noise = np.random.normal(mu, sigma, [2,2])
    #signal = clean_signal + noise

    df.to_csv("/home/ali/IdeaProjects/MD2K_DATA/tmp/dd.csv", sep=',')
    print("*"*40,msg)
    exit()

def iterate_on_rdd(rdd):
    result = rdd.map(lambda msg: add_gaussian_noise(msg))
    print("File Iteration count:", result.count())

def run():

    # create cerebralcortex object
    CC = Kernel("../../conf/", enable_spark_ui=True)

    if CC.config["messaging_service"]=="none":
        raise Exception("Messaging service is disabled (none) in cerebralcortex.yml. Please update configs.")

    # Kafka Consumer Configs
    spark_context = get_or_create_sc(type="sparkContext")

    ssc = StreamingContext(spark_context, int(CC.config["kafka"]["ping_kafka"]))
    kafka_files_stream = CC.MessagingQueue.create_direct_kafka_stream("filequeue", ssc)
    if kafka_files_stream is not None:
        kafka_files_stream.foreachRDD(lambda rdd: iterate_on_rdd(rdd))

    ssc.start()
    ssc.awaitTermination()




if __name__ == "__main__":
    run()
