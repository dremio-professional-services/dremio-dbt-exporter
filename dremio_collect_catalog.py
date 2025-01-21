import dremio_api
import logging
import urllib

logger = logging.getLogger(__name__)


def get_catalog_entries(api: dremio_api.DremioAPI, space_selector=set(), source_selector=[[]]):
    logger.info(f"Retrieving catalog from {api.dremio_url} ...")
    catalog_root = api.get_catalog()
    catalog_entries = collect_dremio_catalog(api, catalog_root, space_selector, source_selector)
    return catalog_entries


def select_source(path: list, source_selector: list[list]) -> bool:
    for s in source_selector:
        if s == path[:len(s)] or path == s[:len(path)]:
            return True
    return False


def collect_dremio_catalog(api: dremio_api.DremioAPI, catalog_root, space_selector: set, source_selector: list[list]) -> list:
    catalog_entries = []
    for entry in catalog_root['data']:
        container_type = entry.get('containerType')
        if container_type == 'SOURCE':
            if not select_source(entry['path'], source_selector):
                logger.info(f"Skipping SOURCE {entry['path']} based on source selector settings.")
            else:
                catalog_id = entry['id']
                logger.info(f"Traversing SOURCE {entry['path']} ...")
                catalog_entries = collect_dremio_catalog_children(api, catalog_entries, catalog_id, data_source_path=entry['path'], source_selector=source_selector)
        elif container_type == 'SPACE':
            catalog_id = entry['id']
            if len(space_selector) > 0 and entry['path'][0] not in space_selector:
                logger.info(f"Skipping SPACE {entry['path']} based on space selector settings.")
            else:
                logger.info(f"Traversing SPACE {entry['path']} ...")
                catalog_entries = collect_dremio_catalog_children(api, catalog_entries, catalog_id)
        else:
            logger.error(f"Unsupported container type {container_type}")
    return catalog_entries


def collect_dremio_catalog_children(api: dremio_api.DremioAPI, data_sources: list, catalog_id, data_source_path=None, source_selector=[[]]) -> list:
    catalog_sub_tree = api.get_catalog(catalog_id)
    try:
        if catalog_sub_tree["entityType"] in ["source", "space"]:
            catalog_sub_tree["path"] = [catalog_sub_tree["name"]]
        data_sources.append({
            "id": catalog_id,
            "object_type": catalog_sub_tree["entityType"],
            "object_path": catalog_sub_tree.get("path", []),
            "parent": [],
            "parent_id": "",
            "parent_type": ""
        })
    except KeyError:
        logger.info(f"Skipping catalog ID {catalog_id}")
    for child in catalog_sub_tree.get('children', []):
        container_type = child.get('containerType')
        dataset_type = child.get('datasetType')
        catalog_id = child['id']
        if child['type'] == 'CONTAINER' and container_type == 'FOLDER':
            if not select_source(child['path'], source_selector):
                logger.info(f"Skipping FOLDER {child['path']} based on source selector settings.") # TODO: set to debug level
            else:
                logger.info(f"Traversing FOLDER {child['path']} ...")
                data_sources = collect_dremio_catalog_children(api, data_sources, catalog_id, data_source_path, source_selector)
        elif child['type'] == 'DATASET' and dataset_type == 'PROMOTED':
            type_name = 'PDS'
            data_sources.append({
                "id": catalog_id,
                "object_type": type_name,
                "object_path": child['path'],
                "parent": data_source_path,
                "parent_id": "",
                "parent_type": "SOURCE"
            })
        elif child['type'] == 'DATASET' and dataset_type == 'VIRTUAL':
            type_name = 'VDS'
            vds_graph = api.get_catalog(catalog_id=f"{catalog_id}/graph")
            try:
                parents = vds_graph['parents']
                if len(parents) == 0:
                    logger.debug(f"No parent objects for view {child['path']} could be found (likely due to RBAC)")
                    data_sources.append({
                        "id": catalog_id,
                        "object_type": type_name, 
                        "object_path": child['path'],
                        "parent": [],
                        "parent_id": "",
                        "parent_type": ""
                    })
                    
                for parent in parents:
                    data_sources.append({
                        "id": catalog_id,
                        "object_type": type_name, 
                        "object_path": child['path'],
                        "parent": parent['path'],
                        "parent_id": parent['id'],
                        "parent_type": parent['datasetType']
                    })
            except KeyError as e:
                logger.error(f"Data lineage for view {child['path']} could not be retrieved")
                data_sources.append({
                    "id": catalog_id,
                    "object_type": type_name, 
                    "object_path": child['path'],
                    "parent": [],
                    "parent_id": "",
                    "parent_type": ""
                })

            # # Add column entries
            # vds_definition = api.get_catalog(vds_id)
            # type_name = 'VDS_column'
            # relationship_table = qualified_name
            # for col in vds_definition['fields']:
            #     name = col['name']
            #     qualified_name = f"{relationship_table}#{name}"
            #     data_type = col['type']['name']
            #     data_sources.append({
            #         "object_type": type_name, 
            #         "object_path": name, 
            #         "fully_qualified_name": qualified_name, 
            #         "parent": relationship_table, 
            #         "type": data_type
            #     })

        elif child['type'] == 'FILE':
            logger.debug(f"Skipping unpromoted file {child['path']}")
        else:
            logger.warning(f"Unsupported container {container_type} or dataset {dataset_type}")
            print(child)
    return data_sources


def generate_catalog_lookup(catalog_entries: list[dict]):
    catalog_lookup = {}
    for entry in catalog_entries:
        catalog_id = entry['id']
        parent_entry = {
            "id": entry['parent_id'],
            "name": entry['parent'],
            "type": entry['parent_type']
        }
        if catalog_id in catalog_lookup:
            catalog_lookup[catalog_id]['parents'].append(parent_entry)
        else:
            catalog_lookup[catalog_id] = {
                "id": catalog_id,
                "object_path": entry['object_path'],
                "object_type": entry['object_type'],
                "parents": [parent_entry]
            }
    return catalog_lookup
