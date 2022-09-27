import os

from infoworks.sdk.cicd.download_configurations.get_source_configuration import DownloadSource
from infoworks.sdk.cicd.download_configurations.lineage import get_lineage_dump
from infoworks.sdk.cicd.download_configurations.utils import Utils
from infoworks.sdk.base_client import BaseClient


class DownloadEntityWithLineage(BaseClient):
    def __init__(self):
        super(DownloadEntityWithLineage, self).__init__()

    def cicd_get_dumps_withlineage(self, workflows_to_dump, pipelines, sources, config_file_dump_path, files_overwrite=True,
                              only_wf_pl=False,
                              maintain_lineage=True,
                              serviceaccountemail="admin@infoworks.io", replace_words=""):
        if not os.path.exists(os.path.join(config_file_dump_path, "modified_files")):
            os.makedirs(os.path.join(config_file_dump_path, "modified_files"))
        if not os.path.exists(os.path.join(config_file_dump_path, "source")):
            os.makedirs(os.path.join(config_file_dump_path, "source"))
        if not os.path.exists(os.path.join(config_file_dump_path, "pipeline")):
            os.makedirs(os.path.join(config_file_dump_path, "pipeline"))
        if not os.path.exists(os.path.join(config_file_dump_path, "workflow")):
            os.makedirs(os.path.join(config_file_dump_path, "workflow"))
        utils_obj = Utils(serviceaccountemail)
        sources_to_dump = []
        pipelines_to_dump = []
        if workflows_to_dump is not None:
            wf_edges = []
            files_dumped = {}
            for workflow_id in workflows_to_dump:
                json_obj = {"entity_id": workflow_id, "entity_type": "workflow"}
                domain_id = utils_obj.get_domain_id(self, json_obj)
                if domain_id:
                    filename, configuration_obj = utils_obj.dump_to_file(self, "workflow", domain_id, workflow_id,
                                                                         replace_words, config_file_dump_path)
                    files_dumped[workflow_id] = filename
                    for task in configuration_obj["configuration"]["workflow"]["workflow_graph"]["tasks"]:
                        if task["task_type"] == "ingest_table_group" and not only_wf_pl:
                            if task["task_properties"]["source_id"] not in sources_to_dump:
                                sources_to_dump.append(task["task_properties"]["source_id"])
                        elif task["task_type"] == "pipeline_build":
                            if task["task_properties"]["pipeline_id"] not in pipelines_to_dump:
                                pipelines_to_dump.append(task["task_properties"]["pipeline_id"])
                        elif task["task_type"] == "build_workflow":
                            if task["task_properties"]["workflow_id"] not in workflows_to_dump:
                                workflows_to_dump.append(task["task_properties"]["workflow_id"])
                                wf_edges.extend([(workflow_id, task["task_properties"]["workflow_id"])])
                        elif task["task_type"] == "export":
                            if task["task_properties"]["export_type"] == "source" and task["task_properties"][
                                "source_id"] not in sources_to_dump and not only_wf_pl:
                                sources_to_dump.append(task["task_properties"]["source_id"])
                            if task["task_properties"]["export_type"] == "pipeline" and task["task_properties"][
                                "pipeline_id"] not in pipelines_to_dump and not only_wf_pl:
                                pipelines_to_dump.append(task["task_properties"]["pipeline_id"])

            if maintain_lineage:
                wf_to_root = []
                for wf in workflows_to_dump:
                    for edge in wf_edges:
                        if wf == edge[-1]:
                            break
                    else:
                        wf_to_root.append(wf)

                workflows_to_dump.append("root")
                for wfid in wf_to_root:
                    wf_edges.append(('root', wfid))
                get_lineage_dump(wf_edges, workflows_to_dump, 'workflow', files_dumped, files_overwrite,
                                 config_file_dump_path)

        if pipelines is not None:
            pipelines_to_dump.extend(pipelines)
            pipelines_to_dump = list(set(pipelines_to_dump))

        if len(pipelines_to_dump) > 0:
            pipeline_edges = []
            files_dumped = {}
            for pipeline_id in pipelines_to_dump:
                json_obj = {"entity_id": pipeline_id, "entity_type": "pipeline"}
                domain_id = utils_obj.get_domain_id(self, json_obj)
                if domain_id:
                    filename, configuration_obj = utils_obj.dump_to_file(self, "pipeline", domain_id, pipeline_id,
                                                                         replace_words, config_file_dump_path)
                    files_dumped[pipeline_id] = filename
                    for node in configuration_obj["configuration"]["pipeline_configs"]["model"].get("nodes", []):
                        req_dict = configuration_obj["configuration"]["pipeline_configs"]["model"]["nodes"][node]
                        if req_dict['type'] == "SOURCE_TABLE":
                            if req_dict['properties']['entity_type'] == "source":
                                src_id = req_dict['properties']['entity_id']
                                if src_id not in sources_to_dump and not only_wf_pl:
                                    sources_to_dump.append(src_id)
                            if req_dict['properties']['entity_type'] == "pipeline_target":
                                pipeline_id_parent = req_dict['properties']['entity_id']
                                if pipeline_id_parent not in pipelines_to_dump and not only_wf_pl:
                                    pipelines_to_dump.append(pipeline_id_parent)
                                pipeline_edges.extend([(pipeline_id_parent, pipeline_id)])

            if maintain_lineage:
                pipeline_to_root = []
                for pipeline in pipelines_to_dump:
                    for edge in pipeline_edges:
                        if pipeline == edge[-1]:
                            break
                    else:
                        pipeline_to_root.append(pipeline)

                pipelines_to_dump.append("root")
                for pid in pipeline_to_root:
                    pipeline_edges.append(('root', pid))
                get_lineage_dump(pipeline_edges, pipelines_to_dump, 'pipeline', files_dumped, files_overwrite,
                                 config_file_dump_path)

        if sources is not None:
            sources_to_dump.extend(sources)
            sources_to_dump = list(set(sources_to_dump))

        if len(sources_to_dump) > 0:
            src_obj = DownloadSource()
            src_obj.cicd_get_sourceconfig_dumps(sources_to_dump, config_file_dump_path, files_overwrite,
                                           serviceaccountemail,
                                           replace_words)
