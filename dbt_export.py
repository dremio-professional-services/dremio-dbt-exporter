import logging
import json
import dremio_api
import dremio_collect_catalog
import os
import re
import sys
import urllib3
urllib3.disable_warnings()

dir_path = os.path.dirname(os.path.realpath(__file__))

# Configure logging
logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s\t%(asctime)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_path_str(view_path: list[str]) -> str:
    s = "_".join(view_path)
    s = s.lower()
    return s



if __name__ == '__main__':

    DREMIO_ENDPOINT = ""
    DREMIO_PAT = ""
    
    api = dremio_api.DremioAPI(DREMIO_PAT, DREMIO_ENDPOINT, timeout=60)

    if True:
        catalog_entries = dremio_collect_catalog.get_catalog_entries(api)
        json_filename = 'dremio_catalog_entries.json'
        with open(json_filename, 'w') as f:
            json.dump(catalog_entries, f)
            logger.info(f"Created {json_filename} with {len(catalog_entries)} entries")

        catalog_lookup = dremio_collect_catalog.generate_catalog_lookup(catalog_entries)
        json_filename = 'dremio_catalog_lookup.json'
        with open(json_filename, 'w') as f:
            json.dump(catalog_lookup, f)
            logger.info(f"Created {json_filename} with {len(catalog_lookup)} entries")

    # job_id = api.post_sql_query('SELECT * FROM sys."tables"')
    # tables = api.get_query_data(job_id)
    # print(tables)

    job_id = api.post_sql_query('SELECT * FROM sys.views')
    views = api.get_query_data(job_id)
    # print(views)

    with open("dremio_catalog_lookup.json", 'r') as f:
        catalog_lookup = json.load(f)

    pdss = []
    sources_yml = {
        'version': 2,
        'sources': []
    }

    for row in views['rows']:
        view_id = row['view_id']
        view_name = row['view_name']
        sql_definition = row['sql_definition']
        sql_context = row['sql_context']
        try:
            parents = catalog_lookup[view_id]['parents']
            view_path = catalog_lookup[view_id]['object_path']
        except Exception as e:
            logger.error(f"Lookup entry not found for {view_name} {row['path']} - {view_id}")
            continue

        dbt_config = {
            'database': view_path[0],
            'schema': view_path[1:-1],
            'alias': view_path[-1],
            'pre_hook': [],
            'post_hook': []
        }

        model_path = str(dir_path) + "/models/" + "/".join(view_path[:-1])
        model_name = model_path + "/" + generate_path_str(view_path) + ".sql"

        logger.info(f"Adding parent references for {view_path}")
        parent_ids = set()
        parent_paths = []
        for p in parents:
            parent_id = p['id']
            try:
                parent = catalog_lookup[parent_id]
            except KeyError as e:
                logger.warning(f"Parent ID {parent_id} for view {view_name} not found in catalog lookup - {e}")
                continue
            p_object_type = parent['object_type']
            parent_path = parent['object_path']
            if parent_id in parent_ids:
                logger.debug(f"Skipping duplicate parent {parent_path}")
                continue
            else:
                parent_ids.add(parent_id)

            if p_object_type == "PDS":
                pdss.append(parent_path)
                dbt_config['pre_hook'].append(parent_path)
                # TODO: Replace PDS references with dbt source() Jinja template
                # TODO: Make sure PDS parent references are not context-sensitve
            elif p_object_type == "VDS":
                parent_path_str = generate_path_str(parent_path)
                parent_paths.append(parent_path_str)
            else:
                raise ValueError(f"Unexpected parent object_type {p_object_type}")

        c = "alias='" + dbt_config['alias'] + "'"
        c += ",\ndatabase='" + dbt_config['database'] + "'"
        if dbt_config['schema']:
            c += ",\nschema='" + ".".join(dbt_config['schema']) + "'"
        if dbt_config['pre_hook']:
            pre_hooks_str = ',\npre_hook=[\n'
            for h in dbt_config['pre_hook']:
                pds_path = '"' + '"."'.join(h) + '"'
                pre_hooks_str += f"    'ALTER PDS {pds_path} REFRESH METADATA AUTO PROMOTION',\n"
            pre_hooks_str = pre_hooks_str[:-2] + '\n]\n'
            c = c + pre_hooks_str

        config_line = '{{ config(' + c + ') }}\n'
        depends_on = ''
        for pp in parent_paths:
            depends_on += "-- depends_on: {{ ref('" + pp + "') }}\n"
        sql_definition = config_line + depends_on + sql_definition

        # create the new directories as needed
        is_exist = os.path.exists(model_path)
        if not is_exist:
            os.makedirs(model_path)

        # write the new model file
        with open(model_name, "w") as file:
            file.write(sql_definition)
    

    data_sources = set()

    # logger.info("\nPDS definitions found:")
    for pds in pdss:
        # logger.info(pds)
        data_sources.add(pds[0])

    with open("pds_paths.json", 'w') as f:
        json.dump(pdss, f)
        logger.info(f"Created pds_paths.json with {len(pdss)} entries")
    
    logger.info("\nData sources found:")
    for d in data_sources:
        logger.info(d)
