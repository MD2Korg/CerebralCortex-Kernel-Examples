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

from cerebralcortex import Kernel
from cerebralcortex.algorithms import gps_clusters
from examples.util.data_helper import gen_location_datastream


class Examples:
    def __init__(self):
        """
        load/set example params/data. This example perform following operations:
            - create sample phone battery data stream
            - perform windowing operation on the stream
            - store windowed data asa new stream
        Args:
            example_name: windowing

        Notes:
            this example generatess ome random gps coordinates of Memphis, TN
        """

        self.setup_example()

        self.gps_clustering()

    def setup_example(self):
        """
        setup required params for the example:
            - create cerebralcortex-kernel object
            - generate sample phone-battery data/metadata
            - create a DataStream object
            - save sample stream using cerebralcortex-kernel.
        """
        # create cerebralcortex object
        self.CC = Kernel("../../conf/")

        # sample data params
        self.stream_name="LOCATION--org.md2k.phonesensor--PHONE"
        self.user_id = "00000000-afb8-476e-9872-6472b4e66b68"

        # generate sample phone-battery data/metadata
        ds = gen_location_datastream(user_id=self.user_id, stream_name=self.stream_name)

        # save sample data using cerebralcortex-kernal.
        # now we have some sample data stored in CerebralCortex format to play with!!!
        self.CC.save_stream(ds)

    def gps_clustering(self):
        # create CC Kernel object

        # get stream data
        gps_stream = self.CC.get_stream("LOCATION--org.md2k.phonesensor--PHONE")

        # apply GPS clustering algorithm
        centroids = gps_stream.groupby("user").compute(gps_clusters)

        # print the centroids
        print("*"*10, " CLUSTER CENTROIDS COORDINATES ", "*"*10)
        centroids.show(truncate=False)

if __name__=="__main__":
    Examples()