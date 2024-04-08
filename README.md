# Infoworks Python SDK

## Introduction
The Infoworks Python library provides convenient access to the Infoworks v3 APIs from
applications written in the Python language. 

It includes pre-defined set of functions performing various actions.

<br>
Supports Infoworks version 5.3 onwards

## Table of Contents
- [Introduction](#introduction)
- [Documentation](#documentation)
- [Installation](#installation)
- [Usage](#usage)
- [Example](#example)
- [Authors](#authors)

![Infoworks SDK Usage GIF](/infoworks_sdk.gif?raw=true)

## Documentation

https://infoworks.github.io/infoworks-python-sdk/

## Installation

You don't need this source code unless you want to modify the package. If you just want to use the package, just run:
```sh
pip install infoworkssdk
```
## Requirements

Python 3.4+ (PyPy supported)

## Usage

The library needs to be configured with your user's refresh token key which is available in your Infoworks UI. Set refresh_token to its value.

### Steps to get refresh token:

https://docs.infoworks.io/infoworks-5.5.0/developer-resources/rest-api#using-refresh-token

```python
from infoworks.sdk.client import InfoworksClientSDK
# Your refresh token here
protocol="https"
host="<your infoworks hostname>"
port="443"
refresh_token = "<your_refresh_token>"
# Initialise the client
iwx_client = InfoworksClientSDK()
iwx_client.initialize_client_with_defaults(protocol=protocol, ip=host, port=port, refresh_token=refresh_token)
```
## Example

Create Oracle Source
```
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

## Authors

Nitin B.S (nitin.bs@infoworks.io)
Abhishek Raviprasad (abhishek.raviprasad@infoworks.io)
