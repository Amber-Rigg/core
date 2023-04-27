# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from ..ami_meters import Usage

def create_usage_VO(uid: str,
                    series_id: str,
                    timestamp: int,
                    interval_timestamp: int,
                    value: float):
    try:
        return Usage(uid = uid,
                        series_id = series_id,
                        timestamp=timestamp,
                        interval_timestamp = interval_timestamp,
                        value = value)

    except Exception as ex:
        error_msg_str: str = 'Could not create Usage Value Object: {}'.format(ex)


