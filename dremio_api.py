import logging
import requests

logger = logging.getLogger(__name__)

class DremioAPI:

    def __init__(self, dremio_pat: str, dremio_url: str, timeout=10, verify=False):
        self.dremio_url = dremio_url.rstrip("/")
        self.timeout = timeout
        self.verify = verify
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + dremio_pat
        }
        # Validate token
        response = requests.request("GET", self.dremio_url + '/api/v3/catalog', headers=self.headers, timeout=self.timeout, verify=self.verify)
        if response.status_code != 200:
            raise Exception(f"Unable to log into {self.dremio_url}. Please validate endpoint and PAT.")

    def get_dataset_id(self, dataset: str):
        dataset_path = dataset.replace(".", "/").replace('"', '')
        url = self.dremio_url + '/api/v3/catalog/by-path/'  + dataset_path

        logger.info(f"Getting ID of {dataset}")
        response = requests.request("GET", url, headers=self.headers, timeout=self.timeout, verify=self.verify)
        data = response.json()
        try:
            dataset_id = data["id"]
        except KeyError:
            logger.warning(data)
            logger.warning(f"Dataset ID for {dataset_path} not found")
            dataset_id = ""
        return dataset_id

    def get_catalog(self, catalog_id=""):
        response = requests.get(
            self.dremio_url + f'/api/v3/catalog/{catalog_id}',
            headers=self.headers, timeout=self.timeout, verify=self.verify
            )
        data = response.json()
        return data

    def get_query_info(self, job_id: str):
        logger.debug("Waiting for job completion...")
        while True:
            response = requests.get(
                self.dremio_url + '/api/v3/job/' + job_id,
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify
            )
            data = response.json()
            job_state = data['jobState']
            if job_state == 'COMPLETED':
                logger.debug('Job successful')
                break
            elif job_state in {"CANCELED", "FAILED"}:
                status = job_state + " - " + data.get("errorMessage", "")
                logger.warning(status)
                break
        return job_state

    def post_sql_query(self, sql: str):
        logger.info(sql)
        response = requests.post(
            self.dremio_url + '/api/v3/sql', 
            headers=self.headers,
            json={"sql": sql },
            timeout=self.timeout, verify=self.verify
        )
        job_id = response.json()['id']
        self.get_query_info(job_id)
        return job_id
    
    def get_query_data(self, job_id: str, limit=500) -> dict:
        job_state = self.get_query_info(job_id)

        if job_state == 'COMPLETED':
            rows = []
            new_rows = ['init_dummy']
            current_offset = 0
            job_results_json = {}
            while len(new_rows) > 0:
                page = 'offset=' + str(current_offset) + '&limit=' + str(limit)
                logger.debug("Paging " + page)
                job_results = requests.get(
                    self.dremio_url + '/api/v3/job/' + job_id + '/results?' + page,
                    headers=self.headers,
                    timeout=self.timeout,
                    verify=self.verify
                )
                if job_results.status_code != 200:
                    Exception(f'Error - {job_results.text}')
                job_results_json = job_results.json()
                new_rows = job_results_json['rows']
                current_offset += len(new_rows)
                rows.extend(new_rows)
            columns = job_results_json.get('columns', [])
            return {"rows": rows, "columns": columns}
        else:
            raise Exception(f'Query data could not be retrieved - Incorrect Job State: {job_state}')

    def get_catalog_tags(self, catalog_id: str) -> list[str]:
        """
        Retrieve tags for a catalog entity by its ID
        
        Args:
            catalog_id: The ID of the catalog entity
            
        Returns:
            A list of tags associated with the catalog entity
        """
        url = self.dremio_url + f'/api/v3/catalog/{catalog_id}/collaboration/tag'
        
        logger.debug(f"Getting tags for catalog ID {catalog_id}")
        response = requests.get(
            url, 
            headers=self.headers, 
            timeout=self.timeout, 
            verify=self.verify
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get('tags', [])
        else:
            logger.debug(f"Failed to get tags for catalog ID {catalog_id}: {response.status_code}")
            return []
