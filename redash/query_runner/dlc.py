import logging
import re

import requests

from redash.query_runner import *
from redash.utils import json_dumps, json_loads

logger = logging.getLogger(__name__)

class DLC(BaseSQLQueryRunner):
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "default": "http://127.0.0.1:8123"
                },
                "appid": {
                    "type": "string",
                    "default": "default"
                },
                "uin": {
                    "type": "string"
                },
                "dbname": {
                    "type": "string",
                    "title": "Database Name"
                },
                "timeout": {
                    "type": "number",
                    "title": "Request Timeout",
                    "default": 30
                }
            },
            "required": ["dbname"],
            "secret": ["password"]
        }

    @classmethod
    def type(cls):
        return "DLC"

    @classmethod
    def name(cls):
        return "DLC"

    @classmethod
    def enabled(cls):
        return True

    def run_query(self, query, user):
        logger.error("DLC is about to execute query: %s", query)
        logger.error("stevensli test DLC.")
        if query == "":
            json_data = None
            error = "Query is empty"
            return json_data, error
        try:
            # q = self._clickhouse_query(query)
            # data = json_dumps(q)
            data = '{"rows": [{"name": "_temporary_and_external_tables"}, {"name": "default"}, {"name": "stevensli"}, {"name": "system"}], "columns": [{"type": "string", "friendly_name": "name", "name": "name"}]}'
            error = None
        except Exception as e:
            data = None
            logging.exception(e)
            error = unicode(e)
        return data, error

register(DLC)
