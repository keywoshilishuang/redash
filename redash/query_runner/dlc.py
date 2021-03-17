import logging
import re

import requests
import json
import base64
import time

from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.dlc.v20210125 import dlc_client, models

logger = logging.getLogger(__name__)


class DLC(BaseSQLQueryRunner):
    noop_query = "show databases"

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
            "required": ["dbname","SecretId","SecretKey"],
            "secret": ["SecretKey"]
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

    def get_dlc_executor(self):
        return dlc_executor(
            self.configuration.get('SecretId', 'default'),
            self.configuration.get('SecretKey', 'default'),
            self.configuration.get('Region', 'ap-beijing'),
            self.configuration.get('dbname', 'default'),
        )

    def get_schema(self, get_stats=False):
        try:
            logger.error("dlc is about to get schema")

            dlcPioneer = self.get_dlc_executor()
            return dlcPioneer.poll_schema_info()
        except Exception as err:
            logger.error("dlc get schema err,%s",err)
            raise err

    def run_query(self, query, user):
        logger.error("DLC is about to execute query: %s user is:%s", query, user)
        if query == "":
            json_data = None
            error = "Query is empty"
            return json_data, error
        try:
            # q = self._clickhouse_query(query)
            # data = json_dumps(q)
            # data = '{"rows": [{"name": "_temporary_and_external_tables"}, {"name": "default"}, {"name": "stevensli"}, {"name": "system"}], "columns": [{"type": "string", "friendly_name": "name", "name": "name"}]}'

            # for dev
            # time.sleep(10)

            result = self._dlc_query(query, user)
            data = json_dumps(result)
            error = None
        except (KeyboardInterrupt, InterruptException):
            logger.error("DLC Query cancelled by user.")
            data = None
            error = "Query cancelled by user."
        except Exception as e:
            data = None
            logging.exception(e)
            error = unicode(e)
        return data, error


    def _dlc_query(self, query, user):
        logger.error("dlc is about to execute query: %s user:%s", query, user)

        dlcPioneer = self.get_dlc_executor()

        try:
            return dlcPioneer.execute(query)

        except TencentCloudSDKException as err:
            logger.error("dlc_query err.")
            logger.exception(err)
            print(err)


class dlc_executor:
    STATE_INIT = 0
    STATE_RUNNING = 1
    STATE_SUCCEEDED = 2
    STATE_WRITING = 3
    STATE_FAILED = -1

    def __init__(self, id, key, region, db):
        self.cred = credential.Credential(str(id), str(key))
        httpProfile = HttpProfile()
        httpProfile.endpoint = "dlc.tencentcloudapi.com"

        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        self.client = dlc_client.DlcClient(self.cred, region, clientProfile)
        self.database = db
        self.poll_interval = 1

    def _reset_state(self):
        self.task_id = None

    def execute(self, query):
        self._reset_state()
        self.task_id = self.createTask(query)

        self._poll(self.task_id)

        return self.resultProcess()

    def createTask(self, query):
        try:
            query = query.split("*/")[-1]
            query = query.strip()
            sql = base64.b64encode(query)
            task = {
                "SQLTask": {
                    "SQL": sql
                }
            }

            req = models.CreateTaskRequest()
            params = {
                "DatabaseName": self.database,
                "Task": task
            }
            req.from_json_string(json.dumps(params))

            resp = self.client.CreateTask(req)
            print(resp.to_json_string())

            return resp.TaskId

        except TencentCloudSDKException as err:
            logger.error("dlc_query TencentCloudSDK err.")
            logger.exception(err)
            print(err)


    def _poll(self, taskId):
        try:
            while True:
                taskInfo = self.describeTask(taskId)
                if taskInfo.State in [
                    dlc_executor.STATE_SUCCEEDED,
                    dlc_executor.STATE_FAILED,
                ]:
                    self.task_Info = taskInfo
                    return
                else:
                    time.sleep(self.poll_interval)

        except Exception as e:
            raise e

    def describeTask(self, taskId):
        try:
            filter = {
                "Name":"task-id",
                "Values":[taskId]
            }

            filters = [filter]


            req = models.DescribeTasksRequest()
            params = {
                "Filters": filters,
            }
            req.from_json_string(json.dumps(params))

            resp = self.client.DescribeTasks(req)
            print(resp.to_json_string())

            return resp.TaskList[0]

        except TencentCloudSDKException as err:
            logger.error("dlc_query err.")
            logger.exception(err)
            print(err)

    def describeTables(self):
        try:
            req = models.DescribeTablesRequest()
            params = {
                "DatabaseName": self.database,
                "Limit":100,
                "Offset":0,
            }
            req.from_json_string(json.dumps(params))

            resp = self.client.DescribeTables(req)
            print(resp.to_json_string())

            return resp.TableList

        except TencentCloudSDKException as err:
            logger.error("dlc_query err.")
            logger.exception(err)
            print(err)

    def describeTable(self, tableName):
        try:
            req = models.DescribeTableRequest()
            params = {
                "DatabaseName": self.database,
                "TableName":tableName,
            }
            req.from_json_string(json.dumps(params))

            resp = self.client.DescribeTable(req)
            print(resp.to_json_string())

            return resp.Table

        except TencentCloudSDKException as err:
            logger.error("dlc_query err.")
            logger.exception(err)
            print(err)

    def poll_schema_info(self):
        try:
            tableList = self.describeTables()

            schema = {}
            for table in tableList:
                # table_name = ""
                table_info = self.describeTable(table)
                if table_info.TableBaseInfo.DatabaseName != self.database:
                    table_name = u'{}.{}'.format(table_info.TableBaseInfo.DatabaseName,
                                                table_info.TableBaseInfo.TableName)
                else:
                    table_name = table_info.TableBaseInfo.TableName

                if table_name in schema:
                    raise Exception("Dlc getting duplicate schema.")

                schema[table_name] = {'name': table_name, 'columns': []}
                for column in table_info.Columns:
                    schema[table_name]['columns'].append(column.Name)

            return schema.values()

        except Exception as err:
            logger.error("Dlc Failed getting schema %s",err)
            raise Exception("Dlc Failed getting schema %s",err)

    def resultProcess(self):
        task = self.task_Info
        columns = []
        rows = []
        if len(task.DataSet) != 0:
            data = json.loads(task.DataSet)

            for cName in data["Schema"]:
                columns.append({'name': cName, 'friendly_name': cName, 'type': "string"})

            # todo
            rows = data["Data"]
        elif len(task.Error) != 0:
            columns.append({'name': "OutputMessage", 'friendly_name': "OutputMessage", 'type': "string"})

            data = task.Error.replace("\n","<br/>")
            data = data.replace(" ", "&nbsp;")
            rows.append({"OutputMessage":data})

        else:
            columns.append({'name': "OutputMessage", 'friendly_name': "OutputMessage", 'type': "string"})

            data = task.RowAffectInfo.replace("\n", "<br/>")
            data = data.replace(" ", "&nbsp;")
            rows.append({"OutputMessage": data})

        return {'columns': columns, 'rows': rows}


register(DLC)
