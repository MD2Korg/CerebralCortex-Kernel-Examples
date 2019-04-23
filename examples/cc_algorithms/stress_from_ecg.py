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
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream import Metadata
from cerebralcortex.algorithms import process_ecg
from cerebralcortex.algorithms import stress_prediction
from cerebralcortex.algorithms import rr_interval_feature_extraction

# Initialize CerebralCortex Kernel
CC = Kernel("../../conf/")

# Get the required input streams from CerebralCortex to compute stress.
ecg_stream = CC.get_stream("ecg--org.md2k.autosenseble--autosense_ble--chest")
ecg_dataquality_stream = CC.get_stream("data_quality--ecg--org.md2k.autosenseble--autosense_ble--chest")

# Merge ECG stream with the ECG data quality stream
ecg_combined_stream = ecg_stream.join(ecg_dataquality_stream, propagation='forward')
"""
+--------------------+--------------------+--------------------+-------+-------+------------+
|                user|           timestamp|           localtime|version|    ecg|data_quality|
+--------------------+--------------------+--------------------+-------+-------+------------+
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|29569.0|           3|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|29569.0|           3|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30537.0|           3|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30537.0|           3|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|29694.0|           3|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|29694.0|           3|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30576.0|           3|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30576.0|           3|
"""

# Filter out the unusable ECG values
filtered_ecg_stream = ecg_combined_stream.filter('data_quality', '==', 0)
'''
+--------------------+--------------------+--------------------+-------+-------+------------+
|                user|           timestamp|           localtime|version|    ecg|data_quality|
+--------------------+--------------------+--------------------+-------+-------+------------+
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30276.0|           0|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30276.0|           0|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30595.0|           0|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30595.0|           0|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30426.0|           0|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30426.0|           0|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30101.0|           0|
'''



#Window the stream for processing ECG data
#The windowing method could be abstracted out to CerebralCortex
windowed_ecg_stream = filtered_ecg_stream.create_windows(window_length='hour')
'''
+--------------------+--------------------+--------------------+-------+-------+------------+-------------+
|                user|           timestamp|           localtime|version|    ecg|data_quality|custom_window|
+--------------------+--------------------+--------------------+-------+-------+------------+-------------+
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30276.0|           0| 2018_3_15_11|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30276.0|           0| 2018_3_15_11|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30595.0|           0| 2018_3_15_11|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30595.0|           0| 2018_3_15_11|
|063b1819-aaa3-436...|2018-03-15 11:47:...|2018-03-15 05:47:...|      1|30426.0|           0| 2018_3_15_11|
'''


# Compute rr_intervals from the ECG data. This is an intermediate stream
rr_intervals = windowed_ecg_stream.compute(process_ecg)
'''
+--------------------+--------------------+-----------+
|                user|           timestamp|rr_interval|
+--------------------+--------------------+-----------+
|063b1819-aaa3-436...|2018-03-15 12:00:...|      1.586|
|063b1819-aaa3-436...|2018-03-15 12:00:...|      1.964|
|063b1819-aaa3-436...|2018-03-15 12:00:...|      1.638|
|063b1819-aaa3-436...|2018-03-15 12:00:...|      1.876|
|063b1819-aaa3-436...|2018-03-15 12:00:...|      1.774|
'''





# Compute rr_interval features. This is an intermediate stream
rr_features = rr_intervals.compute(rr_interval_feature_extraction)
'''
+--------------------+--------------------+--------------------+
|                user|           timestamp|          rr_feature|
+--------------------+--------------------+--------------------+
|063b1819-aaa3-436...|2018-03-15 11:48:...|[0.43653667, 1.73...|
|063b1819-aaa3-436...|2018-03-15 11:49:...|[0.5016612, 0.847...|
|063b1819-aaa3-436...|2018-03-15 11:51:...|[1.0672576, 5.321...|
|063b1819-aaa3-436...|2018-03-15 11:53:...|[0.5873782, 0.498...|
'''





# Predict stress from rr_features. This is an intermediate stream
stress = rr_features.compute(stress_prediction)

stress.show()

'''
+--------------------+--------------------+------------------+
|                user|           timestamp|stress probability|
+--------------------+--------------------+------------------+
|063b1819-aaa3-436...|2018-03-15 11:48:...|         0.0962347|
|063b1819-aaa3-436...|2018-03-15 11:49:...|        0.82236326|
|063b1819-aaa3-436...|2018-03-15 11:51:...|       0.096290246|
|063b1819-aaa3-436...|2018-03-15 11:53:...|        0.08974986|
|063b1819-aaa3-436...|2018-03-15 11:54:...|        0.83813214|
|063b1819-aaa3-436...|2018-03-15 11:55:...|        0.83810747|
|063b1819-aaa3-436...|2018-03-15 11:56:...|        0.09623452|
'''



# Estimate stress episodes from stress predictions
# stress episode generation is currently under development

#stress_episodes = stress.groupby('user').compute(generate_stress_episodes)

#stress_episodes.show()

