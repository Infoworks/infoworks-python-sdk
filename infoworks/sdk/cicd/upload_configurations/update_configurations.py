from typing import List

class InfoworksDynamicAccessNestedDict:
    """Dynamically get/set nested dictionary keys of 'data' dict"""

    def __init__(self, data: dict):
        self.data = data

    def getval(self, keys: List):
        data = self.data
        for k in keys:
            try:
                data = data[k]
            except KeyError as e:
                print(f"Did not find the key {k} in artifact json")
                return None
        return data

    def setval(self, keys: List,rename_dict) -> None:
        data = self.data
        lastkey = keys[-1]
        for k in keys[:-1]:
            data = data[k]
        try:
            if rename_dict.get(data[lastkey].lower(),{})!={} and data[lastkey].lower() in rename_dict.keys():
                data[lastkey] = rename_dict.get(data[lastkey].lower())
        except KeyError as e:
            print(f"Could not find the key {lastkey} in artifact json.Skipping mappings updation...")