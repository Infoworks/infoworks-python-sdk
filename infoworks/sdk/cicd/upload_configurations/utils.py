import json


class Utils:
    def __init__(self):
        pass

    def replace_custom_tags_names_with_mapping(self, data, src_client_obj):
        data['custom_tags'] = []
        for key, value in data.items():
            if key == "custom_tag_key_values":
                for tag_key in value:
                    custom_tag_key = tag_key
                    custom_tag_value = value[tag_key]
                    custom_tag_response = src_client_obj.get_custom_tag_by_key_value(custom_tag_key, custom_tag_value)
                    custom_tag_response_parsed = custom_tag_response.get('result', {}).get('response', {}) \
                        .get('result', [])
                    if custom_tag_response_parsed:
                        custom_tag_id = custom_tag_response_parsed[0]['id']
                        data['custom_tags'].append(custom_tag_id)
                    else:
                        pass
        data.pop('custom_tag_key_values', None)
