# dremio-dbt-exporter
Dremio REST API-based script to export catalog entities as SQL definitions into a dbt-compatible model for Semantic Layer CI/CD and migration between environments.

# Quickstart
To limit scope, in the file `export_filter_sample.json`, set the space names and source folder paths that you want to export:
```
source_selector = [["my-s3-bucket", "folder1"], ["my-adls-source", "folder2"]]
space_selector = {"DremioSpace1", "UseCase2"}
```

Then run 
`python3 dbt_export.py --export-filter-json export_filter_sample.json --dremio-endpoint https://<DREMIO_ENDPOINT> --dremio-pat <INSERT_PAT> [--output-dir foo]`
The results will be exported into the `[foo/]models/` subfolder.

# Requirements
- Python 3
- Dremio Software cluster
- Dremio access token
- Required privileges:
  - User must have `ADMIN` role or have either `SELECT` or `VIEW REFLECTION` privilege on the desired scope
  - User must have `SELECT` privilege on sys.views table or an equivalent view (e.g. `GRANT SELECT ON TABLE sys.views TO USER "<xyz>";`)
  - User must have `SELECT` privilege on sys.reflections table or an equivalent view (e.g. `GRANT SELECT ON TABLE sys.reflections TO USER "<xyz>";`)

# Supported Dremio object types
- Views (VDSs)
- Tables (PDSs)
- Reflections (some syntax limitations may apply)
- Folders (implicit)
- Spaces (implicit)

# Currently not supported
- UDFs (-> Alternative tool: `dremio-udf-recreator`)
- Row- and Column-level access controls (-> Alternative tool: `dremio-udf-recreator`)
- Scripts (-> Alternative tool: `dremio-script-recreator`)
- Wikis & Tags (-> TODO)
- Privileges/RBAC

# Out of scope
- Materialized data, e.g. Iceberg tables (-> ETL workloads should be handled by a separate workflow)
- Sources (-> Not recommended, due to required secrets. Source creation is not supported by dbt-dremio)
- Users and Roles (-> Not recommended. Users and roles should be defined via an external identity provider)


# [Further reading: Whitepaper on Dremio Semantic Layer CI/CD with dbt](https://www.dremio.com/wp-content/uploads/2024/01/Semantic-Layer-CI_CD-with-Dremio-and-dbt.pdf)