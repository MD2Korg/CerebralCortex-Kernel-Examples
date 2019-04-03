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

from cerebralcortex.data_importer.data_parsers import csv_data_parser, mcerebrum_data_parser
from cerebralcortex.data_importer.metadata_parsers import mcerebrum_metadata_parser
from cerebralcortex.data_importer import import_dir
from cerebralcortex.core.metadata_manager.stream import Metadata, DataDescriptor, ModuleMetadata


''' import mCerebrum data '''
# import_dir(
#     cc_config="../../conf/",
#     input_data_dir="sample_data/",
#     metadata_parser=mcerebrum_metadata_parser,
#     data_file_extension=[".gz"],
#     data_parser=mcerebrum_data_parser(),
#     gen_report=True
# )

''' import CSV/IoT data without json metadata files '''
# metadata = Metadata()
# metadata.set_name("SAMPLE-GPS-STREAM-NAME").set_version(1).set_description("GPS sample data stream.") \
#     .add_dataDescriptor(
#     DataDescriptor().set_name("latitude").set_type("float").set_attribute("description", "gps latitude")) \
#     .add_dataDescriptor(
#     DataDescriptor().set_name("longitude").set_type("float").set_attribute("description", "gps longitude")) \
#     .add_dataDescriptor(
#     DataDescriptor().set_name("altitude").set_type("float").set_attribute("description", "gps altitude")) \
#     .add_dataDescriptor(
#     DataDescriptor().set_name("speed").set_type("float").set_attribute("description", "speed info")) \
#     .add_dataDescriptor(
#     DataDescriptor().set_name("bearing").set_type("float").set_attribute("description", "bearing info")) \
#     .add_dataDescriptor(
#     DataDescriptor().set_name("accuracy").set_type("float").set_attribute("description", "accuracy of gps location")) \
#     .add_module(
#     ModuleMetadata().set_name("examples.util.data_helper.gen_location_data").set_version("0.0.1").set_attribute("attribute_key", "attribute_value").set_author(
#         "test_user", "test_user@test_email.com"))
# metadata.is_valid()
#
# import_dir(
#     cc_config="../../conf/",
#     input_data_dir="sample_data/",
#     user_id='976b5f6f-d8d3-47fc-af6a-5ba805918e6d',
#     metadata=metadata,
#     data_file_extension=[".csv"],
#     data_parser=csv_data_parser,
#     gen_report=True
# )


'''import CSV/IoT data'''
import_dir(
    cc_config="../../conf/",
    input_data_dir="sample_data/",
    user_id='976b5f6f-d8d3-47fc-af6a-5ba805918e6d',
    data_file_extension=[".csv"],
    data_parser=csv_data_parser,
    gen_report=True
)


#!!! ----- AVAILABLE PARAMS for import_dir method
'''
cc_config (str): cerebralcortex config directory
input_data_dir (str): data directory path
user_id (str): user id. Currently import_dir only supports parsing directory associated with a user
data_file_extension (list[str]): (optional) provide file extensions (e.g., .doc) that must be ignored
allowed_filename_pattern (str): (optional) regex of files that must be processed.
allowed_streamname_pattern (str): (optional) regex of stream-names to be processed only
ignore_streamname_pattern (str): (optional) regex of stream-names to be ignored during ingestion process 
batch_size (int): (optional) using this parameter will turn on spark parallelism. batch size is number of files each worker will process
compression (str): pass compression name if csv files are compressed
header (str): (optional) row number that must be used to name columns. None means file does not contain any header
metadata (Metadata): (optional) Same metadata will be used for all the data files if this parameter is passed. If metadata is passed then metadata_parser cannot be passed.
metadata_parser (python function): a parser that can parse json files and return a valid MetaData object. If metadata_parser is passed then metadata parameter cannot be passed.
data_parser (python function): a parser than can parse each line of data file. import_dir read data files as a list of lines of a file. data_parser will be applied on all the rows.
gen_report (bool): setting this to True will produce a console output with total failures occurred during ingestion process.
'''