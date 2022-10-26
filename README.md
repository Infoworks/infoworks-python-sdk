# Infoworks Python SDK

The Infoworks Python library provides convenient access to the Infoworks v3 APIs from
applications written in the Python language. 

It includes pre-defined set of functions performing various actions.

<br>
Supports Infoworks version 5.3.x

![alt text](https://github.com/abhr1994/infoworks_sdk/blob/master/infoworks_sdk.gif?raw=true)

# Documentation


# Installation

You don't need this source code unless you want to modify the package. If you just want to use the package, just run:
```sh
pip install infoworkssdk
```
# Requirements

Python 3.4+ (PyPy supported)

# Usage

The library needs to be configured with your user's refresh token key which is available in your Infoworks UI. Set refresh_token to its value:

```python
from infoworks.sdk.client import InfoworksClientSDK
# You refresh token here
refresh_token = "zThziQ7MoJJPYAha+U/+PBSTZG944F+SHBDs+m/z2qn8+m/ax8Prpzla1MHzQ5EBLzB2Bw8a+Qs9r6En5BEN2DsmUVJ6sKFb2yI2"
# Initialise the client
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_user("http", "10.18.1.28", "3001", refresh_token)

# Create Oracle Source

src_create_response = iwx_client.create_source(source_config={
            "name": "iwx_sdk_srcname",
            "type": "rdbms",
            "sub_type": "oracle",
            "data_lake_path": "/iw/sources/iwx_sdk_srcname",
            "environment_id": "",
            "storage_id": "",
            "is_source_ingested": True
        })
```
