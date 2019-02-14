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

from pyspark.sql import functions as F
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from examples.util.data_helper import gen_phone_battery_data, gen_phone_battery_metadata

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata

class Examples:
    def __init__(self):
        """
        load/set example params/data. This example perform following operations:
            - create sample phone battery data stream
            - perform windowing operation on the stream
            - store windowed data asa new stream
        Args:
            example_name: windowing
        """

        self.setup_example()

        self.basic_datastream_operations()

    def setup_example(self):
        """
        setup required params for the example:
            - create cerebralcortex-kernel object
            - generate sample phone-battery data/metadata
            - create a DataStream object
            - save sample stream using cerebralcortex-kernel.
        """
        # create cerebralcortex object
        self.CC = CerebralCortex("../../conf/")

        # sample data params
        self.stream_name="BATTERY--org.md2k.phonesensor--PHONE"
        self.user_id = "00000000-afb8-476e-9872-6472b4e66b68"

        # generate sample phone-battery data/metadata
        data = gen_phone_battery_data(user_id=self.user_id)
        metadata = gen_phone_battery_metadata(stream_name=self.stream_name)

        # create a DataStream object
        ds = DataStream(data, metadata)

        # save sample data using cerebralcortex-kernal.
        # now we have some sample data stored in CerebralCortex format to play with!!!
        self.CC.save_stream(ds)

    def basic_datastream_operations(self):
        """
        This example will window phone battery stream into 1 minutes chunks and take the average of battery level

        """

        # get sample stream data
        ds = self.CC.get_stream(self.stream_name)
        metadata = ds.get_metadata(version=1)
        print(metadata)
        # {'name': 'BATTERY--org.md2k.phonesensor--PHONE', 'version': 1, 'description': [''], 'metadata_ha.........

        ds.show(3,False)
        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |timestamp          |offset  |battery_level|ver|version|user                                |
        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |2019-01-09 11:49:28|21600000|92           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:49:29|21600000|92           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:49:30|21600000|92           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # +-------------------+--------+-------------+---+-------+------------------------------------+

        ds.filter("battery_level", ">", 97)
        ds.show(3,False)
        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |timestamp          |offset  |battery_level|ver|version|user                                |
        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |2019-01-09 11:39:08|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:39:09|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:39:10|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # +-------------------+--------+-------------+---+-------+------------------------------------+

        ds.filter_user("00000000-afb8-476e-9872-6472b4e66b68")
        ds.show(3,False)
        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |timestamp          |offset  |battery_level|ver|version|user                                |
        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |2019-01-09 11:39:08|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:39:09|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:39:10|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # +-------------------+--------+-------------+---+-------+------------------------------------+

        ds.filter_version("1")
        ds.show(3,False)

        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |timestamp          |offset  |battery_level|ver|version|user                                |
        # +-------------------+--------+-------------+---+-------+------------------------------------+
        # |2019-01-09 11:39:08|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:39:09|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # |2019-01-09 11:39:10|21600000|98           |1  |1      |00000000-afb8-476e-9872-6472b4e66b68|
        # +-------------------+--------+-------------+---+-------+------------------------------------+

        ds.window(windowDuration=60)
        ds.show(3, False)
        # +------------------------------------+------------------------------------------+---------------------------------
        # |user                                |window                                    |collect_list(battery_level)
        # +------------------------------------+------------------------------------------+---------------------------------
        # |00000000-afb8-476e-9872-6472b4e66b68|[2019-01-09 11:35:00, 2019-01-09 11:36:00]|[100, 100, 100, 100, 100.........
        # |00000000-afb8-476e-9872-6472b4e66b68|[2019-01-09 11:38:00, 2019-01-09 11:39:00]|[99, 99, 99, 99, 99, 99, 99,.....
        # |00000000-afb8-476e-9872-6472b4e66b68|[2019-01-09 11:39:00, 2019-01-09 11:40:00]|[98, 98, 98, 98, 98, 98, 98,.....
        # +------------------------------------+------------------------------------------+---------------------------------

        ds.to_pandas()
        print(ds.data)
        # user  ...                                 collect_list(user)
        # 0  00000000-afb8-476e-9872-6472b4e66b68  ...  [00000000-afb8-476e-9872-6472b4e66b68, 0000000...
        # 1  00000000-afb8-476e-9872-6472b4e66b68  ...  [00000000-afb8-476e-9872-6472b4e66b68, 0000000...
        # 2  00000000-afb8-476e-9872-6472b4e66b68  ...  [00000000-afb8-476e-9872-6472b4e66b68, 0000000...
        # 3  00000000-afb8-476e-9872-6472b4e66b68  ...  [00000000-afb8-476e-9872-6472b4e66b68, 0000000...
        # 4  00000000-afb8-476e-9872-6472b4e66b68  ...  [00000000-afb8-476e-9872-6472b4e66b68, 0000000...

if __name__=="__main__":
    Examples()

