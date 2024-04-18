import json
import logging
from typing import List

import pydantic_core
import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_api_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        api_endpoint_url = f"{self.api_base_url}/processed_agent_data"
        processed_data = [data.model_dump_json() for data in processed_agent_data_batch]
        response = requests.post(api_endpoint_url, '[' + ','.join(processed_data) + ']')

        if response.status_code != requests.codes.ok:
            return False
        else:
            return True