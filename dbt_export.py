import argparse
import logging
import json
import dremio_api
import dremio_collect_catalog
import os
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
    s = s.replace(" ", "")
    s = s.lower()
    return s


def write_catalog_entries_to_file(api: dremio_api.DremioAPI, space_selector=set(), source_selector=set()) -> list[dict]:

    catalog_entries = dremio_collect_catalog.get_catalog_entries(api, space_selector, source_selector)
    json_filename = 'dremio_catalog_entries.json'
    with open(os.path.join(dir_path, json_filename), 'w') as f:
        json.dump(catalog_entries, f)
        logger.info(f"Created {json_filename} with {len(catalog_entries)} entries")

    return catalog_entries


def write_catalog_lookup_to_file(catalog_entries) -> dict[dict]:

    catalog_lookup = dremio_collect_catalog.generate_catalog_lookup(catalog_entries)
    json_filename = 'dremio_catalog_lookup.json'
    with open(os.path.join(dir_path, json_filename), 'w') as f:
        json.dump(catalog_lookup, f)
        logger.info(f"Created {json_filename} with {len(catalog_lookup)} entries")

    return catalog_lookup


def generate_parent_refs(view_path, parents: list[dict], catalog_lookup: dict[dict]) -> list[str]:
    logger.debug(f"Adding parent references for {view_path}")
    parent_ids = set()
    parent_paths = []
    for p in parents:
        parent_id = p['id']
        try:
            parent = catalog_lookup[parent_id]
        except KeyError as e:
            logger.warning(f"Parent ID {parent_id} for view {view_path} not found in catalog lookup - {e}")
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
        elif p_object_type == "VDS":
            parent_path_str = generate_path_str(parent_path)
            parent_paths.append(parent_path_str)
        else:
            raise ValueError(f"Unsupported parent object_type {p_object_type}")
    
    return parent_paths


def generate_config(dbt_config: dict[str], parent_paths: list[str]) -> str:
    c = ""
    if dbt_config.get('alias'):
        c += "alias='" + dbt_config['alias'] + "'"
    if dbt_config.get('database'):
        c += ",\ndatabase='" + dbt_config['database'] + "'"
    if dbt_config.get('schema'):
        c += ",\nschema='" + ".".join(dbt_config['schema']) + "'"
    if dbt_config.get('pre_hook'):
        pre_hooks_str = ',\npre_hook=[\n'
        for h in dbt_config['pre_hook']:
            pds_path = '"' + '"."'.join(h) + '"'
            pre_hooks_str += f"    'ALTER PDS {pds_path} REFRESH METADATA AUTO PROMOTION',\n"
        pre_hooks_str = pre_hooks_str[:-2] + '\n]\n'
        c += pre_hooks_str

    # For reflections
    if dbt_config.get('reflection_type'):
        c += "materialized='reflection'"
        c += ",\nreflection_type='" + dbt_config['reflection_type'] + "'"
        if dbt_config['reflection_type'] == "aggregate":
            logger.warning(f"Please validate agg reflection measure computations for {parent_paths}, as this is currently not supported!"
                           "\nSee: https://github.com/dremio-professional-services/dremio-dbt-exporter/issues/5")
    if dbt_config.get('reflection_name'):
        c += ",\nname='" + dbt_config['reflection_name'] + "'"
    if dbt_config.get('display'):
        cols = str(dbt_config['display'].split(', '))
        c += ",\ndisplay=" + cols
    if dbt_config.get('dimensions'):
        cols = str(dbt_config['dimensions'].split(', '))
        c += ",\ndimensions=" + cols
    if dbt_config.get('measures'):
        cols = str(dbt_config['measures'].split(', '))
        c += ",\nmeasures=" + cols
    # TODO: computations
    if dbt_config.get('localsort_by'):
        cols = str(dbt_config['localsort_by'].split(', '))
        c += ",\nlocalsort_by=" + cols
    if dbt_config.get('partition_by'):
        cols = str(dbt_config['partition_by'].split(', '))
        c += ",\npartition_by=" + cols

    config_line = '{{ config(' + c + ') }}\n'
    depends_on = ''
    for pp in parent_paths:
        depends_on += "-- depends_on: {{ ref('" + pp + "') }}\n"
    
    config = config_line + depends_on
    return config


def build_sys_views_filter(space_selector: set) -> str:
    s = ""
    if len(space_selector) > 0:
        s += "WHERE FALSE"
        for space in space_selector:
            s += f"\n   OR path LIKE '[{space}%' "
    return s


def build_sys_reflections_filter(space_selector: set) -> str:
    s = ""
    if len(space_selector) > 0:
        s += "WHERE FALSE"
        for space in space_selector:
            s += f"\n   OR dataset_name LIKE '{space}%' OR dataset_name LIKE '\"{space}%' "
    return s

def parse_cli_args():
    parser = argparse.ArgumentParser(description='Dremio dbt exporter')
    parser.add_argument('--export-filter-json', type=str,
                        help='Absolute path to export_filter.json file.',
                        required=True)
    parser.add_argument('--dremio-endpoint', type=str, help='Dremio URL incl. https:// prefix', required=True)
    parser.add_argument('--dremio-pat', type=str, help='Dremio PAT', required=True)
    parser.add_argument('--output-dir', type=str, help='Output directory of dbt models', required=False)
    cli_args = parser.parse_args()
    return cli_args

if __name__ == '__main__':

    args = parse_cli_args()
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = dir_path

    with open(args.export_filter_json, 'r') as f:
        d = json.load(f)
        source_selector = d["source_selector"]
        space_selector = d["space_selector"]

    api = dremio_api.DremioAPI(args.dremio_pat, args.dremio_endpoint, timeout=60)

    if True:
        catalog_entries = write_catalog_entries_to_file(api, space_selector, source_selector)
        catalog_lookup = write_catalog_lookup_to_file(catalog_entries)
    else: # for local debugging
        # with open("dremio_catalog_entries.json", 'r') as f:
        #     catalog_entries = json.load(f)
        with open("dremio_catalog_lookup.json", 'r') as f:
            catalog_lookup = json.load(f)

    # Retrieve full list of views and SQL definitions from system table
    where_clause = build_sys_views_filter(space_selector)
    job_id = api.post_sql_query('SELECT * FROM sys.views ' + where_clause)
    views = api.get_query_data(job_id)

    pdss = []

    for row in views['rows']:
        view_id = row['view_id']
        view_name = row['view_name']
        sql_definition = row['sql_definition']
        sql_context: str = row['sql_context']
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

        model_path = str(output_dir) + "/models/" + "/".join(view_path[:-1])
        model_name = model_path + "/" + generate_path_str(view_path) + ".sql"

        parent_paths = generate_parent_refs(view_path, parents, catalog_lookup)

        config = generate_config(dbt_config, parent_paths)
        sql_definition = config + sql_definition
        if sql_context:
            logger.warn(f"Found SQL context {sql_context} in view {view_path}")
            context = str(sql_context.split('.')) # Note that this logic does not handle special cases like "Samples"."samples.dremio.com"
            sql_definition += "\n--SQL_CONTEXT=" + sql_context

        # create the new directories as needed
        if not os.path.exists(model_path):
            os.makedirs(model_path)

        # write the new model file
        with open(model_name, "w") as file:
            file.write(sql_definition)
    
    # Retrieve full list of reflections and SQL definitions from system table
    where_clause = build_sys_reflections_filter(space_selector)
    job_id = api.post_sql_query('SELECT * FROM sys.reflections ' + where_clause)
    reflections = api.get_query_data(job_id)

    for r in reflections['rows']:
        reflection_name = r['reflection_name']
        reflection_id = r['reflection_id']
        reflection_type = r['type']
        dataset_id = r['dataset_id']
        dataset_name = r['dataset_name']
        dataset_type = r['dataset_type']
        display_columns = r['display_columns']
        sort_columns = r['sort_columns']
        partition_columns = r['partition_columns']
        dimensions = r['dimensions']
        measures = r['measures']
        try:
            dataset_path = catalog_lookup[dataset_id]['object_path']
            ref = generate_path_str(dataset_path)
        except Exception as e:
            #logger.error(f"Lookup entry not found for {dataset_name} - {dataset_id}")
            continue

        if reflection_type == 'RAW':
            refl_type = 'raw'
        elif reflection_type == 'AGGREGATION':
            refl_type = 'aggregate'
        else:
            logger.error(f"Unsupported reflection type {reflection_type} for dataset {dataset_name}")
            continue

        dbt_config = {
            'reflection_name': reflection_name,
            'reflection_type': refl_type,
            'display': display_columns,
            'dimensions': dimensions,
            'measures': measures,
            'computations': None, # TODO
            'localsort_by': sort_columns,
            'partition_by': partition_columns,
        }
        config = generate_config(dbt_config, [ref])

        refl_path = str(output_dir) + "/models/" + "/".join(dataset_path[:-1])
        refl_name = refl_path + "/REFL_" + reflection_name.replace(" ", "").lower() + "_" + reflection_id[:8] + ".sql"

        with open(refl_name, "w") as file:
            file.write(config)

    data_sources = set()

    # logger.info("\nPDS definitions found:")
    for pds in pdss:
        # logger.info(pds)
        data_sources.add(pds[0])

    with open(os.path.join(dir_path, "pds_promote.sql"), 'w') as f:
        sql_txt = ""
        for pds in pdss:
            pds_path = '"' + '"."'.join(pds) + '"'
            sql_txt += f'ALTER PDS {pds_path} REFRESH METADATA AUTO PROMOTION;\n'
        f.write(sql_txt)
        logger.info(f"Created pds_promote.sql with {len(pdss)} entries")
    
    logger.info("Data sources found:")
    for d in data_sources:
        logger.info(d)
