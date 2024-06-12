# dremio-dbt-exporter
Dremio REST API-based script to export catalog entities as SQL definitions into a dbt-compatible model

In the file `dbt_export.py`, set the following variables:
```
DREMIO_ENDPOINT = "https://<DREMIO_ENDPOINT>"
DREMIO_PAT = "<INSERT_DREMIO_ADMIN_PAT>" 
```

Then run `python3 dbt_export.py`.
The results will be exported into the `models/` subfolder.

# Requirements
- Dremio Software cluster
- Dremio ADMIN account and access token
- Python 3

# Supported Dremio object types
- Views (VDSs)
- Tables (PDSs)
- Reflections
- Folders (implicit)
- Spaces (implicit)

# Currently not supported:
- UDFs
- Row- and Column-level access controls
- Users and Roles
- Privileges (RBAC)

# [Further reading on Dremio Semantic Layer definition in dbt](https://www.dremio.com/wp-content/uploads/2024/01/Semantic-Layer-CI_CD-with-Dremio-and-dbt.pdf)