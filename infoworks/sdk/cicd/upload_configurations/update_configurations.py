import json
import re
from typing import List
import copy
# def get_keys(d, curr_key=[]):
#     if isinstance(d,str):
#         return
#     for k, v in d.items():
#         if isinstance(v, dict):
#             yield from get_keys(v, curr_key + [k])
#         elif isinstance(v, list):
#             for i in v:
#                 yield from get_keys(i, curr_key + [k])
#         else:
#             yield '.'.join(curr_key + [k])

def get_keys(d, curr_key=[]):
    if isinstance(d,str):
        return
    for k, v in d.items():
        if isinstance(v, dict):
            yield from get_keys(v, curr_key + [k])
        elif isinstance(v, list):
            for index,i in enumerate(v):
                yield from get_keys(i, curr_key +[k]+[str(index)])
        else:
            yield '.'.join(curr_key + [k])

class InfoworksDynamicAccessNestedDict:
    """Dynamically get/set nested dictionary keys of 'data' dict"""

    def __init__(self, data: dict):
        self.data = data
        self.flatten_json_list = [*get_keys(data)]

    def getval(self, keys: List):
        replacement_string = ".".join(keys)
        r = re.compile(replacement_string)
        matched_replacements = list(filter(r.match, self.flatten_json_list))
        data = self.data
        for item in matched_replacements:
            section_keys=item.split(".")
            for k in section_keys:
                try:
                    data = data[k]
                except KeyError as e:
                    print(f"Did not find the key {k} in artifact json")
                    return None
            return data

    def setval(self, keys: List,rename_dict) -> None:
        replacement_string = ".".join(keys)
        r=re.compile(replacement_string)
        matched_replacements = list(filter(r.match, self.flatten_json_list))
        for key in matched_replacements:
            section_keys = key.split(".")
            data = self.data
            lastkey = section_keys[-1]
            for k in section_keys[:-1]:
                if k.isdigit():
                    k = int(k)
                data = data[k]
            try:
                if rename_dict.get(data[lastkey].lower(),{})!={} and data[lastkey].lower() in rename_dict.keys():
                    data[lastkey] = rename_dict.get(data[lastkey].lower())
            except KeyError as e:
                print(f"Could not find the key {lastkey} in artifact json.Skipping mappings updation...")