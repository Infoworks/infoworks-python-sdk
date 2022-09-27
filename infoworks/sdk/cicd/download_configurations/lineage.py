import os
from uuid import uuid4
import networkx as nx


def get_lineage_dump(edges, entities, entity_type, files_dumped, files_overwrite, config_file_dump_path):
    G = nx.DiGraph()
    G.add_edges_from(edges)
    nodes = [(i, {"filename": files_dumped.get(i, "")}) for i in entities]
    G.add_nodes_from(nodes)
    target_file_path = os.path.join(os.path.join(config_file_dump_path, "modified_files"), f"{entity_type}.csv")
    target_file_path_gexf = os.path.join(os.path.join(config_file_dump_path, "modified_files"), f"{entity_type+'_'+str(uuid4())}.gexf")
    if files_overwrite:
        open(target_file_path, 'w').close()
        mode = "a"
    else:
        mode = "a"
    nx.write_gexf(G, target_file_path_gexf)
    f = open(target_file_path, mode)
    for i in nx.descendants(G, 'root'):  # Returns tree from top to bottom
        file_name = files_dumped.pop(i, None)
        if file_name is not None:
            f.write(file_name)
            f.write("\n")
    f.close()
