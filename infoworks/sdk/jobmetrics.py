import copy
import datetime
import math
import traceback
from collections import OrderedDict
from infoworks.sdk import url_builder
from infoworks.sdk.utils import IWUtils
from infoworks.error import AdminError
from infoworks.sdk.base_client import BaseClient
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


class JobMetricsClient(BaseClient):
    job_type_mappings = {
        'EXPORT_DATA': 'EXPORT',
        'SOURCE_CRAWL': 'FULL_LOAD',
        'SOURCE_CDC': 'INCREMENTAL',
        'SOURCE_CDC_MERGE': 'FULL_LOAD',
        'SOURCE_STRUCTURED_CRAWL': 'FULL_LOAD',
        'FULL_LOAD': 'FULL_LOAD',
        'CDC': 'INCREMENTAL',
        'MERGE': 'INCREMENTAL',
        'SOURCE_MERGE': 'INCREMENTAL',
        'SOURCE_STRUCTURED_CDC_MERGE': 'INCREMENTAL',
        'SOURCE_STRUCTURED_CDC': 'INCREMENTAL',
        'SOURCE_SEMISTRUCTURED_CDC_MERGE': 'INCREMENTAL',
        'SOURCE_SEMISTRUCTURED_CDC': 'INCREMENTAL'
    }

    def __init__(self):
        super(JobMetricsClient, self).__init__()
        self.job_metrics_final = []

    def get_tablegroup_id_from_job(self, job_id, source_id):
        try:
            url_to_get_jobinfo = url_builder.submit_source_job(self.client_config, source_id) + f"/{job_id}"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_jobinfo,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result")
                table_group_id = result.get("sub_entity_id", "")
                processedAt = result.get("processed_at", "")
                job_end_time = result.get("last_updated", "")
                status = result.get("status", "")
                entity_type = result.get("entity_type", "")
                return table_group_id, processedAt, job_end_time, status, entity_type
        except Exception as e:
            raise AdminError("Unable to get table group details from the Job Id")

    def get_table_group_name(self, table_group_id, source_id):
        try:
            url_to_get_tginfo = url_builder.create_table_group_url(self.client_config, source_id) + f"/{table_group_id}"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_tginfo,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", {})
                return result.get('name'), result.get('tables')
        except Exception as e:
            raise AdminError("Unable to get table group info")

    def get_ingestion_metrics(self, job_id, source_id):
        """
        Function to get ingestion metrics for the job
        :param job_id: Entity identifier of the job
        :param source_id: Entity identifier of the source
        :return: List of job metrics
        """
        try:
            combinedJobMetric = []
            url_to_get_job_ingestion_metrics = url_builder.get_job_metrics_url(self.client_config, source_id,
                                                                               job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_job_ingestion_metrics,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobMetric
        except Exception as e:
            raise AdminError("Unable to get ingestion job metrics info")

    def get_source_file_paths(self, source_id, table_id, job_id):
        try:
            source_files = []
            url_to_get_src_filepaths = url_builder.get_source_file_paths_url(self.client_config, source_id, table_id,
                                                                             job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_src_filepaths,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    source_files.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return source_files
        except Exception as e:
            print(e)
            raise AdminError("Unable to get source file pat metrics info")

    def get_export_metrics(self, job_id):
        try:
            combinedJobMetric = []
            url_to_get_export_metrics = url_builder.get_export_metrics_url(self.client_config,
                                                                           job_id) + "?limit=50&offset=0"

            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_export_metrics,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobMetric
        except Exception as e:
            print(e)
            raise AdminError("Unable to get export job metrics info")

    def get_pipeline_build_metrics(self, job_id):
        try:
            combinedJobMetric = []
            url_to_get_pipeline_build_metrics = url_builder.get_pipeline_build_metrics_url(self.client_config,
                                                                                   job_id) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_pipeline_build_metrics,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobMetric.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobMetric
        except Exception as e:
            raise AdminError("Unable to get pipeline job metrics info")

    def get_source_info(self):
        try:
            combined_sources = []
            url_to_get_get_source_info = url_builder.get_source_details_url(
                self.client_config) + "?limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_get_source_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combined_sources.extend(result)
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=response.get('links')['next'],
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combined_sources
        except Exception as e:
            raise AdminError("Unable to get source details")

    def get_jobs_of_single_source(self, source_id, date_string):
        try:
            combinedJobs = []
            # filter_condition = "{\"$and\": [{\"createdAt\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"jobType\": {\"$in\": [\"source_crawl\", \"source_cdc_merge\"]}}]}"
            filter_condition = "{\"$and\": [{\"entityId\":\"" + source_id + "\"},{\"last_upd\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"jobType\": {\"$in\": [\"source_crawl\",\"source_structured_crawl\",\"source_cdc\",\"source_cdc_merge\",\"export_data\",\"full_load\",\"cdc\",\"source_merge\",\"source_structured_cdc_merge\",\"source_structured_cdc\",\"source_cdc_merge\",\"source_semistructured_cdc_merge\",\"source_semistructured_cdc\"]}}, {\"status\":{\"$in\":[\"failed\",\"completed\",\"running\",\"pending\",\"canceled\",\"aborted\"]}}]}"
            url_to_get_jobs = url_builder.get_job_status_url(
                self.client_config) + f"?filter={filter_condition}&limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)

            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobs.extend(result)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobs
        except Exception as e:
            raise AdminError("Unable to get ingestion jobs list of source")

    def get_pipeline_jobs(self, date_string):
        try:
            combinedJobs = []
            filter_condition = "{\"$and\": [{\"last_upd\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"jobType\": {\"$in\": [\"pipeline_build\"]}}, {\"status\":{\"$in\":[\"failed\",\"completed\",\"running\",\"pending\",\"aborted\",\"canceled\"]}}]}"
            url_to_get_jobs = url_builder.get_job_status_url(
                self.client_config) + f"?filter={filter_condition}&limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)

            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobs.extend(result)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobs
        except Exception as e:
            raise AdminError("Unable to get pipeline jobs list")

    def get_all_source_jobs(self, date_string):
        try:
            combinedJobs = []
            filter_condition = "{\"$and\": [{\"last_upd\": {\"$gte\": {\"$date\": \"" + date_string + "\"}}},{\"entityType\": {\"$in\": [\"source\"]}}, {\"jobType\": {\"$in\": [\"source_crawl\",\"source_structured_crawl\",\"source_cdc\",\"source_cdc_merge\",\"export_data\",\"full_load\",\"cdc\",\"source_merge\",\"source_structured_cdc_merge\",\"source_structured_cdc\",\"source_cdc_merge\",\"source_semistructured_cdc_merge\",\"source_semistructured_cdc\"]}},{\"status\":{\"$in\":[\"failed\",\"completed\",\"running\",\"pending\",\"canceled\",\"aborted\"]}}]}"
            url_to_get_all_source_jobs = url_builder.get_job_status_url(
                self.client_config) + f"?filter={filter_condition}&limit=50&offset=0"
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_all_source_jobs,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", [])
                while len(result) > 0:
                    combinedJobs.extend(result)
                    next_url = response.get('links')['next']
                    nextUrl = '{protocol}://{ip}:{port}{next}'.format(next=next_url,
                                                                      ip=self.client_config['ip'],
                                                                      port=self.client_config['port'],
                                                                      protocol=self.client_config['protocol'],
                                                                      )
                    response = IWUtils.ejson_deserialize(self.callurl(nextUrl).content)
                    result = response.get("result", [])
                return combinedJobs
        except Exception as e:
            raise AdminError("Unable to get source ingestion jobs list")

    def callurl(self, url):
        try:
            response = self.call_api("GET", url,
                                     IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                                     )
            if response is not None:
                return response
        except Exception as e:
            raise AdminError("Unable to get response for url: {url}".format(url=url))

    def get_table_info(self, source_id, table_id):
        try:
            url_to_get_table_info = url_builder.get_table_configuration(
                self.client_config, source_id, table_id)
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_table_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None:
                result = response.get("result", {})
                return result
        except Exception as e:
            raise AdminError("Unable to get table info")

    def get_table_export_info(self, source_id, table_id):
        try:
            url_to_get_table_info = url_builder.table_export_config_url(
                self.client_config, source_id, table_id)
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_table_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", {})
                return result
        except Exception as e:
            raise AdminError("Unable to get export job metrics info")

    def get_pipeline_name(self, domain_id, pipeline_id):
        try:
            url_to_get_pipeline_info = url_builder.get_pipeline_url(self.client_config, domain_id, pipeline_id)
            response = IWUtils.ejson_deserialize(
                self.call_api("GET", url_to_get_pipeline_info,
                              IWUtils.get_default_header_for_v3(self.client_config['bearer_token'])
                              ).content)
            if response is not None and "result" in response:
                result = response.get("result", None)
                return result.get('name')
        except:
            raise AdminError("Unable to get pipeline NAME")

    def get_source_jobs_metrics_results(self, date_string, source, workflow_id=None, workflow_run_id=None):
        src_name = source["name"]
        list_of_jobs_obj = self.get_jobs_of_single_source(source["id"], date_string)
        try:
            for job in list_of_jobs_obj:
                job_id = job["id"]
                job_type = job["type"]
                job_status = job["status"]
                job_createdAt = job["created_at"]
                running_job_template = {
                    'workflow_id': job.get('triggered_by', {}).get('parent_id', ''),
                    'workflow_run_id': job.get('triggered_by', {}).get('run_id', ''),
                    'job_id': job_id,
                    'job_type': self.job_type_mappings[job_type.upper()],
                    'job_start_time': job_createdAt.split('.')[0].replace('T', ' '),
                    'job_end_time': "",
                    "source_file_names": [],
                    'job_status': job_status.upper(),
                    'entity_type': "source",
                    "source_name": src_name, "source_schema_name": job.get("source_schema_name", ""),
                    "source_database_name": job.get("source_database_name", ""), "table_group_name": "",
                    "iwx_table_name": "", 'starting_watermark_value': '', 'ending_watermark_value': '',
                    "pre_target_count": "", "fetch_records_count": 0,
                    "target_records_count": ""}
                running_od = OrderedDict()
                if job_status.upper() in ["RUNNING", "PENDING", "CANCELED", "ABORTED", "FAILED"]:
                    for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                                'job_end_time', 'job_status',
                                'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                                'table_group_name',
                                'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value',
                                'target_schema_name', 'target_table_name', 'pre_target_count', 'fetch_records_count',
                                'target_records_count']:
                        running_od[key] = running_job_template.get(key, "")
                    self.job_metrics_final.append(running_od)
                    continue
                try:
                    tg_id, processedAt, job_end_time, job_status, entity_type = self.get_tablegroup_id_from_job(
                        job_id,
                        source["id"])
                    table_group_name, all_tables_list = self.get_table_group_name(tg_id, source["id"])
                except:
                    # This means the job is non-tablegroup job
                    table_group_name = ""
                    entity_type = "TABLE"
                    all_tables_list = []
                ing_metrics = self.get_ingestion_metrics(str(job_id), source["id"])
                successful_tables = []
                if ing_metrics != [] and job_type != "export_data":
                    tables_list = list(set([i['table_id'] for i in ing_metrics]))
                    df = pd.DataFrame(ing_metrics)
                    for table_id in tables_list:
                        successful_tables.append(table_id)
                        filter1 = df["table_id"] == table_id
                        table = {}
                        if len(df.loc[filter1]) > 1:
                            # Incremental Job
                            filter2 = df["job_type"] == "CDC"
                            filter3 = df["job_type"] == "MERGE"
                            cdc_output = df.loc[filter1 & filter2].to_dict('records')[0]
                            merge_output = df.loc[filter1 & filter3].to_dict('records')[0]
                            for item in ['source_id', 'fetch_records_count', 'job_id', 'job_start_time',
                                         'source_schema_name', 'source_database_name']:
                                table[item] = cdc_output.get(item, "")
                                table['job_type'] = "INCREMENTAL"
                            for item in ['workflow_id', 'workflow_run_id', 'job_end_time', 'job_status',
                                         'target_records_count', 'first_merged_watermark', 'last_merged_watermark']:
                                table[item] = merge_output.get(item, "")
                                table['job_type'] = "INCREMENTAL"
                        else:
                            table = df.loc[filter1].to_dict('records')[0]
                        table_name_url = self.get_table_info(table.get('source_id'), table_id)
                        table_name = table_name_url.get('name')
                        target_table_name = table_name_url.get("configuration", {}).get('target_table_name', '')
                        target_schema_name = table_name_url.get("configuration", {}).get('target_schema_name', '')
                        table["source_name"] = src_name
                        table["source_schema_name"] = table.get("source_schema_name", "")
                        table["source_database_name"] = table.get("source_database_name", "")
                        table["source_file_names"] = self.get_source_file_paths(source['id'], table_id,
                                                                                job_id) if table[
                                                                                               "source_schema_name"] == "" else []
                        table["table_group_name"] = table_group_name
                        table["iwx_table_name"] = table_name
                        table["target_schema_name"] = target_schema_name
                        table["target_table_name"] = target_table_name
                        table["workflow_id"] = table.get("workflow_id", "")
                        table["workflow_run_id"] = table.get('workflow_run_id', "")
                        if workflow_id is not None and workflow_run_id is not None:
                            if not (table["workflow_id"] == workflow_id and table[
                                "workflow_run_id"] == workflow_run_id):
                                continue
                        table["entity_type"] = entity_type
                        table["starting_watermark_value"] = table.pop('first_merged_watermark', '')
                        table["ending_watermark_value"] = table.pop('last_merged_watermark', '')
                        if math.isnan(table.get('target_records_count')):
                            table['pre_target_count'] = None
                            table['target_records_count'] = None
                        else:
                            if table["job_status"] == "FAILED":
                                table['pre_target_count'] = table.get('target_records_count')
                                table['target_records_count'] = int(table.get('target_records_count'))
                            else:
                                table['pre_target_count'] = int(
                                    table.get('target_records_count') - int(table.get('fetch_records_count')))
                                table['target_records_count'] = int(table.get('target_records_count'))
                        if table.get('job_type') == "CDC":
                            table['job_type'] = "INCREMENTAL"
                        od = OrderedDict()
                        table['job_start_time'] = table['job_start_time'].split('.')[0].replace('T', ' ')
                        table['job_end_time'] = table['job_end_time'].split('.')[0].replace('T', ' ')
                        table['fetch_records_count'] = int(table['fetch_records_count'])
                        for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type',
                                    'job_start_time',
                                    'job_end_time', 'job_status',
                                    'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                                    'table_group_name',
                                    'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value',
                                    'target_schema_name', 'target_table_name', 'pre_target_count',
                                    'fetch_records_count',
                                    'target_records_count']:
                            od[key] = table.get(key, '')
                        self.job_metrics_final.append(od)

                for table in all_tables_list:
                    if table['table_id'] not in successful_tables and job_type != "export_data":
                        tableInfo = self.get_table_info(source["id"], table['table_id'])
                        table_name = tableInfo.get("name")
                        row_count = tableInfo.get('row_count', 0)
                        synctype = tableInfo.get('configuration').get('sync_type')
                        temp_failed = OrderedDict()
                        temp = {
                            'workflow_id': job.get('triggered_by', {}).get('parent_id', ''),
                            'workflow_run_id': job.get('triggered_by', {}).get('run_id', ''),
                            'job_id': job_id,
                            'job_type': synctype.upper(),
                            'job_start_time': processedAt.split('.')[0].replace('T', ' '),
                            'job_end_time': job_end_time.split('.')[0].replace('T', ' '),
                            'job_status': job_status.upper(),
                            'entity_type': entity_type,
                            "source_file_names": [],
                            "target_table_name": tableInfo.get("target_table_name", ""),
                            "target_schema_name": tableInfo.get("target_schema_name", ""),
                            "source_name": src_name, "source_schema_name": job.get("source_schema_name", ""),
                            "source_database_name": job.get("source_database_name", ""),
                            "table_group_name": table_group_name, "iwx_table_name": table_name,
                            'starting_watermark_value': '', 'ending_watermark_value': '',
                            "pre_target_count": row_count, "fetch_records_count": 0,
                            "target_records_count": row_count}
                        for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type',
                                    'job_start_time',
                                    'job_end_time', 'job_status',
                                    'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                                    'table_group_name',
                                    'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value',
                                    'target_schema_name', 'target_table_name', 'pre_target_count',
                                    'fetch_records_count',
                                    'target_records_count']:
                            temp_failed[key] = temp.get(key, "")

                        self.job_metrics_final.append(temp_failed)

                if job_type == "export_data":
                    export_metrics = self.get_export_metrics(str(job_id))
                    export_successful_tables = []
                    if export_metrics is not None:
                        tables_list = list([i['table_id'] for i in export_metrics])
                        df_export = pd.DataFrame(export_metrics)
                        for table_id in tables_list:
                            export_successful_tables.append(table_id)
                            filter1 = df_export["table_id"] == table_id
                            table = df_export.loc[filter1].to_dict('records')[0]
                            table["job_type"] = "EXPORT"
                            table_name_url = self.get_table_info(table.get('source_id'), table_id)
                            table_export_config = self.get_table_export_info(table.get('source_id'), table_id)
                            table_name = table_name_url.get('name')
                            table["source_name"] = src_name
                            table["table_group_name"] = table_group_name
                            table["iwx_table_name"] = table_name
                            if table_export_config.get("target_type", "") == "BIGQUERY":
                                table["target_schema_name"] = ".".join(
                                    [table_export_config.get("connection", {}).get("project_id", ""),
                                     table_export_config.get("target_configuration", {}).get("dataset_name", "")])
                            else:
                                table["target_schema_name"] = ".".join(
                                    [table_export_config.get("target_configuration", {}).get("schema_name", ""),
                                     table_export_config.get("target_configuration", {}).get("database_name", "")])
                            table["target_table_name"] = table_export_config.get("target_configuration", {}).get(
                                "table_name", "")
                            table["workflow_id"] = table.get("workflow_id", "")
                            table["workflow_run_id"] = table.get("workflow_run_id", "")
                            table["entity_type"] = entity_type
                            table["source_file_names"] = []
                            table["starting_watermark_value"] = table.pop('first_merged_watermark', '')
                            table["ending_watermark_value"] = table.pop('last_merged_watermark', '')
                            if math.isnan(table.get('target_records_count')):
                                table['pre_target_count'] = None
                                table['target_records_count'] = None
                            else:
                                if table["job_status"] == "FAILED":
                                    table['pre_target_count'] = table.get('target_records_count')
                                    table['target_records_count'] = int(table.get('target_records_count'))
                                else:
                                    table['pre_target_count'] = int(
                                        table.get('target_records_count') - int(table.get('number_of_records_written')))
                                    table['target_records_count'] = int(table.get('target_records_count'))
                            od = OrderedDict()
                            table['job_start_time'] = table['job_start_time'].split('.')[0].replace('T', ' ')
                            table['job_end_time'] = table['job_end_time'].split('.')[0].replace('T', ' ')
                            table['fetch_records_count'] = int(table['number_of_records_written'])
                            for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type',
                                        'job_start_time', 'job_end_time', 'job_status',
                                        'source_name', 'source_file_names', 'source_schema_name',
                                        'source_database_name',
                                        'table_group_name',
                                        'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value',
                                        'target_schema_name', 'target_table_name', 'pre_target_count',
                                        'fetch_records_count',
                                        'target_records_count']:
                                od[key] = table.get(key, "")
                            self.job_metrics_final.append(od)

                    for table in all_tables_list:
                        if table['table_id'] not in export_successful_tables:
                            tableInfo = self.get_table_info(source["id"], table['table_id'])
                            table_name = tableInfo.get("name")
                            row_count = tableInfo.get('row_count', 0)
                            export_failed_od = OrderedDict()
                            temp = {
                                'workflow_id': job.get('triggered_by', {}).get('parent_id', ''),
                                'workflow_run_id': job.get('triggered_by', {}).get('run_id', ''),
                                'job_id': job_id,
                                'job_type': "EXPORT",
                                'job_start_time': processedAt.split('.')[0].replace('T', ' '),
                                'job_end_time': job_end_time.split('.')[0].replace('T', ' '),
                                'job_status': job_status.upper(),
                                'entity_type': entity_type,
                                "source_file_names": [],
                                "source_name": src_name, "table_group_name": table_group_name,
                                "iwx_table_name": table_name,
                                'starting_watermark_value': '', 'ending_watermark_value': '',
                                "pre_target_count": row_count, "fetch_records_count": 0,
                                "target_records_count": row_count}
                            for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type',
                                        'job_start_time', 'job_end_time', 'job_status',
                                        'source_name', 'source_file_names', 'source_schema_name',
                                        'source_database_name',
                                        'table_group_name',
                                        'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value',
                                        'target_schema_name', 'target_table_name', 'pre_target_count',
                                        'fetch_records_count',
                                        'target_records_count']:
                                export_failed_od[key] = temp.get(key, "")
                            self.job_metrics_final.append(export_failed_od)
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def get_pipeline_build_metrics_results(self, job, workflow_id=None, workflow_run_id=None):
        temp = []
        job_id = job['id']
        job_type = job['type']
        job_status = job['status']
        job_createdAt = job['created_at']
        entity_type = job['entity_type']
        running_job_template = {
            'workflow_id': job.get('triggered_by', {}).get('parent_id', ''),
            'workflow_run_id': job.get('triggered_by', {}).get('run_id', ''),
            'job_id': job_id,
            'job_type': job_type.upper(),
            'job_start_time': job_createdAt.split('.')[0].replace('T', ' '),
            'job_end_time': "",
            "source_file_names": [],
            'job_status': job_status.upper(),
            'entity_type': entity_type,
            "source_name": "", "source_schema_name": job.get("source_schema_name", ""),
            "source_database_name": job.get("source_database_name", ""), "table_group_name": "", "iwx_table_name": "",
            'starting_watermark_value': '', 'ending_watermark_value': '',
            "pre_target_count": "", "fetch_records_count": 0,
            "target_records_count": ""}
        running_od = OrderedDict()
        if job_status.upper() in ["RUNNING", "PENDING", "CANCELED", "ABORTED", "FAILED"]:
            for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                        'job_end_time', 'job_status',
                        'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                        'table_group_name',
                        'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value', 'target_schema_name',
                        'target_table_name', 'pre_target_count', 'fetch_records_count',
                        'target_records_count']:
                running_od[key] = running_job_template.get(key, "")
            self.job_metrics_final.append(running_od)
            return
        pipeline_metrics = self.get_pipeline_build_metrics(str(job_id))
        pipeline_successful_tables = []
        if pipeline_metrics is not None:
            tables_list = list(set([i['target_table_name'] for i in pipeline_metrics]))
            df_pipeline = pd.DataFrame(pipeline_metrics)
            for target_table_name in tables_list:
                schema_name, table_name = target_table_name.split(".")
                pipeline_successful_tables.append(table_name)
                filter1 = df_pipeline["target_table_name"] == target_table_name
                table = df_pipeline.loc[filter1].to_dict('records')[0]
                table["job_type"] = "PIPELINE_BUILD"
                table["source_name"] = ''
                table["source_file_names"] = []
                table["source_schema_name"] = ""
                table["source_database_name"] = ""
                table["table_group_name"] = ''
                table["iwx_table_name"] = ''
                table['target_schema_name'] = schema_name
                table['target_table_name'] = table_name
                table["workflow_id"] = table.get('workflow_id', '')
                table["workflow_run_id"] = table.get('workflow_run_id', '')
                if workflow_id is not None and workflow_run_id is not None:
                    if not (table["workflow_id"] == workflow_id and table[
                        "workflow_run_id"] == workflow_run_id):
                        continue
                table["entity_type"] = job.get("entity_type", "pipeline")
                table["starting_watermark_value"] = table.pop('first_merged_watermark', '')
                table["ending_watermark_value"] = table.pop('last_merged_watermark', '')
                table["target_records_count"] = table.get("target_records_count", None)
                table["job_status"] = table.get("job_status", "")
                if math.isnan(table.get('target_records_count')):
                    table['pre_target_count'] = None
                    table['target_records_count'] = None
                else:
                    if table["job_status"] == "FAILED":
                        table['pre_target_count'] = table.get('target_records_count')
                        table['target_records_count'] = int(table.get('target_records_count'))
                    else:
                        table['pre_target_count'] = int(
                            table.get('target_records_count') - int(table.get('number_of_records_written')))
                        table['target_records_count'] = int(table.get('target_records_count'))
                    od = OrderedDict()
                    table['job_start_time'] = table['job_start_time'].split('.')[0].replace('T', ' ')
                    table['job_end_time'] = table['job_end_time'].split('.')[0].replace('T', ' ')
                    table['fetch_records_count'] = int(table['number_of_records_written'])
                    for key in ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time',
                                'job_end_time', 'job_status',
                                'source_name', 'source_file_names', 'source_schema_name', 'source_database_name',
                                'table_group_name',
                                'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value',
                                'target_schema_name', 'target_table_name', 'pre_target_count', 'fetch_records_count',
                                'target_records_count']:
                        od[key] = table.get(key, "")
                    temp.append(od)
            self.job_metrics_final.extend(temp)

    def get_abc_job_metrics(self, time_range_for_jobs_in_mins, workflow_id=None, workflow_run_id=None):
        try:
            sources_info = self.get_source_info()
            delay = int(time_range_for_jobs_in_mins)
            now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=delay)
            date_string = now.strftime('%Y-%m-%dT%H:%M:%SZ')
            source_jobs = self.get_all_source_jobs(date_string)
            source_jobs_source_ids = list(set([i['entity_id'] for i in source_jobs]))
            sources_info = [i for i in sources_info if i['id'] in source_jobs_source_ids]
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(self.get_source_jobs_metrics_results,
                             [date_string] * len(sources_info), sources_info,
                             [workflow_id, workflow_run_id] * len(sources_info))
                executor.shutdown(wait=True)
            pipeline_jobs_list = self.get_pipeline_jobs(date_string)
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(self.get_pipeline_build_metrics_results, pipeline_jobs_list,
                             [workflow_id, workflow_run_id] * len(pipeline_jobs_list))
                executor.shutdown(wait=True)

            result = []
            header = ['workflow_id', 'workflow_run_id', 'job_id', 'entity_type', 'job_type', 'job_start_time', 'job_end_time', 'job_status', 'source_name', 'source_file_names', 'source_schema_name', 'source_database_name', 'table_group_name', 'iwx_table_name', 'starting_watermark_value', 'ending_watermark_value', 'target_schema_name', 'target_table_name', 'pre_target_count', 'fetch_records_count', 'target_records_count', 'job_link']
            if len(self.job_metrics_final) > 0:
                for item in self.job_metrics_final:
                    dict_temp = {}
                    for i in header:
                        dict_temp[i] = item.get(i)
                    result.append(copy.deepcopy(dict_temp))
                return result
            else:
                print("Job list empty!!!")
        except Exception as e:
            print("Something went wrong")
            print(str(e))
            traceback.print_exc()
