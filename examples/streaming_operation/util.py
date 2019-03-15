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


import requests
import json
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata

def login_user(url, username, password):
    try:
        data = {"username": str(username),"password": str(password)}
        headers = {"Accept": "application/json"}
        response = requests.post(url, json=data, headers=headers)

        return json.loads(response.content)
    except Exception as e:
        raise Exception("Login failed. "+str(e))

def register_stream(url, auth_token, stream_metadata):
    try:
        headers = {"Accept": "application/json", "Authorization": auth_token}
        response = requests.post(url, json=stream_metadata, headers=headers)
        return json.loads(response.content)
    except Exception as e:
        raise Exception("Login failed. "+str(e))

def upload_data(base_url, username, password, stream_metadata, data_file_path):
    login_url = base_url+"api/v3/user/login"
    register_stream_url = base_url+"api/v3/stream/register"

    auth = login_user(login_url, username, password)
    status = register_stream(register_stream_url, auth.get("auth_token"), stream_metadata)

    stream_upload_url = base_url+"api/v3/stream/"+status.get("hash_id")

    try:
        f = open(data_file_path, "rb")
        files = {"file": (data_file_path, f)}

        headers = {"Accept": "application/json", "Authorization": auth.get("auth_token")}
        response = requests.put(stream_upload_url, files=files, headers=headers)

        return json.loads(response.content)
    except Exception as e:
        raise Exception("Login failed. "+str(e))


def rest_api_client(api_url):
    stream_name = "accelerometer--org.md2k.phonesensor--phone"

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

    upload_data(api_url, "string", "string", metadata.to_json(), "/home/ali/IdeaProjects/MD2K_DATA/msgpack/6-5300c809-8c16-4576-b467-d638a609d4d8.msgpack.gz")

rest_api_client("http://0.0.0.0:8089/")