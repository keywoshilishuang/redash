import logging
import re

import requests
import json
import base64

from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.dlc.v20210125 import dlc_client, models

logger = logging.getLogger(__name__)

class DLC(BaseSQLQueryRunner):
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "Region": {
                    "type": "string",
                    "default": "ap-beijing"
                },
                "SecretId": {
                    "type": "string"
                },
                "SecretKey": {
                    "type": "string"
                },
                "dbname": {
                    "type": "string",
                    "title": "Database Name"
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
        logger.error("DLC is about to execute query: %s user is:%s", query, user)
        logger.error("stevensli test DLC.")
        if query == "":
            json_data = None
            error = "Query is empty"
            return json_data, error
        try:
            # q = self._clickhouse_query(query)
            # data = json_dumps(q)
            self._dlc_query(query, user)
            data = '{"rows": [{"name": "_temporary_and_external_tables"}, {"name": "default"}, {"name": "stevensli"}, {"name": "system"}], "columns": [{"type": "string", "friendly_name": "name", "name": "name"}]}'
            error = None
        except Exception as e:
            data = None
            logging.exception(e)
            error = unicode(e)
        return data, error

    def _dlc_query(self, query, user):
        logger.error("dlc is about to execute query: %s user:%s", query, user)

        dlcPioneer = dlc_executor(
            self.configuration.get('SecretId', 'default'),
            self.configuration.get('SecretKey', 'default'),
            self.configuration.get('Region', 'default'),
            self.configuration.get('dbname', 'default'),
        )

        try:
            query = query[query.rindex("*/") + 2:]
            query = query.strip()

            sql = base64.b64encode(query)

            task = {
                "SQLTask":{
                    "SQL":sql
                }
            }

            req = models.CreateTaskRequest()
            params = {
                "DatabaseName": dlcPioneer.database,
                "Task": task
            }
            req.from_json_string(json.dumps(params))

            resp = dlcPioneer.client.CreateTask(req)
            print(resp.to_json_string())

        except TencentCloudSDKException as err:
            logger.error("dlc_query err.")
            logger.exception(err)
            print(err)


class dlc_executor:
    def __init__(self, id, key, region, db):
        self.cred = credential.Credential(str(id), str(key))
        httpProfile = HttpProfile()
        httpProfile.endpoint = "dlc.tencentcloudapi.com"

        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        self.client = dlc_client.DlcClient(self.cred, region, clientProfile)
        self.database = db

    def createTask(self, query, user):
        req = models.CreateTaskRequest()
        # params


register(DLC)
