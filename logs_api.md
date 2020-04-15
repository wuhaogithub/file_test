# 日志接口

## 日志全文检索接口

基于elasticsearch全文索引，可以根据DAG_ID,TASK_ID,EXECUTION_DATE,EXTRA等字段，对所有日志信息进行精确帅选，也可以根据需求添加其他的筛选字段。es_task_handler.py  是一个从Elasticsearch中读取日志的python日志处理程序。注意:日志没有直接索引到Elasticsearch中。相反，它将日志刷新到本地文件中。需要额外的软件设置来将日志索引到Elasticsearch中，比如使用Filebeat和Logstash。

为了有效地查询和排序Elasticsearch的结果，我们假设每个日志消息都有一个字段‘log_id’，它由ti主键组成:‘log_id = {dag_id}-{task_id}-{execution_date}-{try_number}’，具有特定log_id的日志消息根据‘offset’进行排序，‘offset’是一个惟一的整数，表示日志消息的顺序。这里的时间戳是不可靠的，因为多个日志消息可能具有相同的时间戳。

```python
import logging
import sys
import elasticsearch
import pendulum
from elasticsearch_dsl import Search
from airflow.configuration import conf
from airflow.utils import timezone
from airflow.utils.helpers import parse_template_string
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.json_formatter import JSONFormatter
from airflow.utils.log.logging_mixin import LoggingMixin


class ElasticsearchTaskHandler(FileTaskHandler, LoggingMixin):
    """
    ElasticsearchTaskHandler is a python log handler that
    reads logs from Elasticsearch. Note logs are not directly
    indexed into Elasticsearch. Instead, it flushes logs
    into local files. Additional software setup is required
    to index the log into Elasticsearch, such as using
    Filebeat and Logstash.
    To efficiently query and sort Elasticsearch results, we assume each
    log message has a field `log_id` consists of ti primary keys:
    `log_id = {dag_id}-{task_id}-{execution_date}-{try_number}`
    Log messages with specific log_id are sorted based on `offset`,
    which is a unique integer indicates log message's order.
    Timestamp here are unreliable because multiple log messages
    might have the same timestamp.
    """

    PAGE = 0
    MAX_LINE_PER_PAGE = 1000

    def __init__(self, base_log_folder, filename_template,
                 log_id_template, end_of_log_mark,
                 write_stdout, json_format, json_fields,
                 host='localhost:9200',
                 es_kwargs=conf.getsection("elasticsearch_configs")):
        """
        :param base_log_folder: base folder to store logs locally
        :param log_id_template: log id template
        :param host: Elasticsearch host name
        """
        es_kwargs = es_kwargs or {}
        super().__init__(
            base_log_folder, filename_template)
        self.closed = False

        self.log_id_template, self.log_id_jinja_template = \
            parse_template_string(log_id_template)

        self.client = elasticsearch.Elasticsearch([host], **es_kwargs)

        self.mark_end_on_close = True
        self.end_of_log_mark = end_of_log_mark
        self.write_stdout = write_stdout
        self.json_format = json_format
        self.json_fields = [label.strip() for label in json_fields.split(",")]
        self.handler = None
        self.context_set = False

    def _render_log_id(self, ti, try_number):
        if self.log_id_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return self.log_id_jinja_template.render(**jinja_context)

        if self.json_format:
            execution_date = self._clean_execution_date(ti.execution_date)
        else:
            execution_date = ti.execution_date.isoformat()
        return self.log_id_template.format(dag_id=ti.dag_id,
                                           task_id=ti.task_id,
                                           execution_date=execution_date,
                                           try_number=try_number)

    @staticmethod
    def _clean_execution_date(execution_date):
        """
        Clean up an execution date so that it is safe to query in elasticsearch
        by removing reserved characters.
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters

        :param execution_date: execution date of the dag run.
        """
        return execution_date.strftime("%Y_%m_%dT%H_%M_%S_%f")

    def _read(self, ti, try_number, metadata=None):
        """
        Endpoint for streaming log.

        :param ti: task instance object
        :param try_number: try_number of the task instance
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: a list of log documents and metadata.
        """
        if not metadata:
            metadata = {'offset': 0}
        if 'offset' not in metadata:
            metadata['offset'] = 0

        offset = metadata['offset']
        log_id = self._render_log_id(ti, try_number)

        logs = self.es_read(log_id, offset, metadata)

        next_offset = offset if not logs else logs[-1].offset

        # Ensure a string here. Large offset numbers will get JSON.parsed incorrectly
        # on the client. Sending as a string prevents this issue.
        # https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER
        metadata['offset'] = str(next_offset)

        # end_of_log_mark may contain characters like '\n' which is needed to
        # have the log uploaded but will not be stored in elasticsearch.
        metadata['end_of_log'] = False if not logs \
            else logs[-1].message == self.end_of_log_mark.strip()

        cur_ts = pendulum.now()
        # Assume end of log after not receiving new log for 5 min,
        # as executor heartbeat is 1 min and there might be some
        # delay before Elasticsearch makes the log available.
        if 'last_log_timestamp' in metadata:
            last_log_ts = timezone.parse(metadata['last_log_timestamp'])
            if cur_ts.diff(last_log_ts).in_minutes() >= 5 or 'max_offset' in metadata \
                    and offset >= metadata['max_offset']:
                metadata['end_of_log'] = True

        if offset != next_offset or 'last_log_timestamp' not in metadata:
            metadata['last_log_timestamp'] = str(cur_ts)

        # If we hit the end of the log, remove the actual end_of_log message
        # to prevent it from showing in the UI.
        i = len(logs) if not metadata['end_of_log'] else len(logs) - 1
        message = '\n'.join([log.message for log in logs[0:i]])

        return message, metadata

    def es_read(self, log_id, offset, metadata):
        """
        Returns the logs matching log_id in Elasticsearch and next offset.
        Returns '' if no log is found or there was an error.

        :param log_id: the log_id of the log to read.
        :type log_id: str
        :param offset: the offset start to read log from.
        :type offset: str
        :param metadata: log metadata, used for steaming log download.
        :type metadata: dict
        """

        # Offset is the unique key for sorting logs given log_id.
        search = Search(using=self.client) \
            .query('match_phrase', log_id=log_id) \
            .sort('offset')

        search = search.filter('range', offset={'gt': int(offset)})
        max_log_line = search.count()
        if 'download_logs' in metadata and metadata['download_logs'] and 'max_offset' not in metadata:
            try:
                if max_log_line > 0:
                    metadata['max_offset'] = search[max_log_line - 1].execute()[-1].offset
                else:
                    metadata['max_offset'] = 0
            except Exception:  # pylint: disable=broad-except
                self.log.exception('Could not get current log size with log_id: %s', log_id)

        logs = []
        if max_log_line != 0:
            try:

                logs = search[self.MAX_LINE_PER_PAGE * self.PAGE:self.MAX_LINE_PER_PAGE] \
                    .execute()
            except Exception as e:  # pylint: disable=broad-except
                self.log.exception('Could not read log with log_id: %s, error: %s', log_id, str(e))

        return logs

    def set_context(self, ti):
        """
        Provide task_instance context to airflow task handler.

        :param ti: task instance object
        """
        self.mark_end_on_close = not ti.raw

        if self.json_format:
            self.formatter = JSONFormatter(
                self.formatter._fmt,  # pylint: disable=protected-access
                json_fields=self.json_fields,
                extras={
                    'dag_id': str(ti.dag_id),
                    'task_id': str(ti.task_id),
                    'execution_date': self._clean_execution_date(ti.execution_date),
                    'try_number': str(ti.try_number)
                })

        if self.write_stdout:
            if self.context_set:
                # We don't want to re-set up the handler if this logger has
                # already been initialized
                return

            self.handler = logging.StreamHandler(stream=sys.__stdout__)
            self.handler.setLevel(self.level)
            self.handler.setFormatter(self.formatter)
        else:
            super().set_context(ti)
        self.context_set = True

    def close(self):
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if not self.mark_end_on_close:
            self.closed = True
            return

        # Case which context of the handler was not set.
        if self.handler is None:
            self.closed = True
            return

        # Reopen the file stream, because FileHandler.close() would be called
        # first in logging.shutdown() and the stream in it would be set to None.
        if self.handler.stream is None or self.handler.stream.closed:
            self.handler.stream = self.handler._open()  # pylint: disable=protected-access

        # Mark the end of file using end of log mark,
        # so we know where to stop while auto-tailing.
        self.handler.stream.write(self.end_of_log_mark)

        if self.write_stdout:
            self.handler.close()
            sys.stdout = sys.__stdout__

        super().close()

        self.closed = True
```

## 本地logs保存设置接口

可以使用`base_log_folder`设置在`airflow.cfg`指定日志文件夹。 默认情况下，它位于`AIRFLOW_HOME`目录中。

file_processor_handler.py是一个处理dag处理器日志的python日志处理程序。它创建并将日志处理委托给“日志记录”。接收dag处理器上下文后的文件处理程序。

```python
import logging
import os
from datetime import datetime

from airflow import settings
from airflow.utils.helpers import parse_template_string


class FileProcessorHandler(logging.Handler):
    """
    FileProcessorHandler is a python log handler that handles
    dag processor logs. It creates and delegates log handling
    to `logging.FileHandler` after receiving dag processor context.

    :param base_log_folder: Base log folder to place logs.
    :param filename_template: template filename string
    """

    def __init__(self, base_log_folder, filename_template):
        super().__init__()
        self.handler = None
        self.base_log_folder = base_log_folder
        self.dag_dir = os.path.expanduser(settings.DAGS_FOLDER)
        self.filename_template, self.filename_jinja_template = \
            parse_template_string(filename_template)

        self._cur_date = datetime.today()
        if not os.path.exists(self._get_log_directory()):
            try:
                os.makedirs(self._get_log_directory())
            except OSError:
                # only ignore case where the directory already exist
                if not os.path.isdir(self._get_log_directory()):
                    raise

                logging.warning("%s already exists", self._get_log_directory())

        self._symlink_latest_log_directory()

    def set_context(self, filename):
        """
        Provide filename context to airflow task handler.

        :param filename: filename in which the dag is located
        """
        local_loc = self._init_file(filename)
        self.handler = logging.FileHandler(local_loc)
        self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)

        if self._cur_date < datetime.today():
            self._symlink_latest_log_directory()
            self._cur_date = datetime.today()

    def emit(self, record):
        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        if self.handler is not None:
            self.handler.close()

    def _render_filename(self, filename):
        filename = os.path.relpath(filename, self.dag_dir)
        ctx = dict()
        ctx['filename'] = filename

        if self.filename_jinja_template:
            return self.filename_jinja_template.render(**ctx)

        return self.filename_template.format(filename=ctx['filename'])

    def _get_log_directory(self):
        now = datetime.utcnow()

        return os.path.join(self.base_log_folder, now.strftime("%Y-%m-%d"))

    def _symlink_latest_log_directory(self):
        """
        Create symbolic link to the current day's log directory to
        allow easy access to the latest scheduler log files.

        :return: None
        """
        log_directory = self._get_log_directory()
        latest_log_directory_path = os.path.join(self.base_log_folder, "latest")
        if os.path.isdir(log_directory):  # pylint: disable=too-many-nested-blocks
            try:
                # if symlink exists but is stale, update it
                if os.path.islink(latest_log_directory_path):
                    if os.readlink(latest_log_directory_path) != log_directory:
                        os.unlink(latest_log_directory_path)
                        os.symlink(log_directory, latest_log_directory_path)
                elif (os.path.isdir(latest_log_directory_path) or
                      os.path.isfile(latest_log_directory_path)):
                    logging.warning(
                        "%s already exists as a dir/file. Skip creating symlink.",
                        latest_log_directory_path
                    )
                else:
                    os.symlink(log_directory, latest_log_directory_path)
            except OSError:
                logging.warning("OSError while attempting to symlink "
                                "the latest log directory")

    def _init_file(self, filename):
        """
        Create log file and directory if required.

        :param filename: task instance object
        :return: relative log path of the given task instance
        """
        relative_path = self._render_filename(filename)
        full_path = os.path.join(self._get_log_directory(), relative_path)
        directory = os.path.dirname(full_path)

        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except OSError:
                if not os.path.isdir(directory):
                    raise

        if not os.path.exists(full_path):
            open(full_path, "a").close()

        return full_path
```

## 标准日志接口

可根据web前端界面需求，返回给前端相应的数据;

stackdriver_task_handler.py是一个Python标准的“日志”处理程序，可以用来将Python标准的日志消息直接路由到Stackdriver日志API。它还可以用于保存执行任务的日志。为此，您应该将其设置为名称为“tasks”的处理程序。在本例中，它还将用于读取Web UI中显示的日志。此处理程序同时支持异步和同步传输。

```python
"""
Handler that integrates with Stackdriver
"""
import logging
from typing import Dict, List, Optional, Tuple, Type

from cached_property import cached_property
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import logging as gcp_logging
from google.cloud.logging.handlers.transports import BackgroundThreadTransport, Transport
from google.cloud.logging.resource import Resource

from airflow import version
from airflow.models import TaskInstance

DEFAULT_LOGGER_NAME = "airflow"
_GLOBAL_RESOURCE = Resource(type="global", labels={})


class StackdriverTaskHandler(logging.Handler):
    """Handler that directly makes Stackdriver logging API calls.

    This is a Python standard ``logging`` handler using that can be used to
    route Python standard logging messages directly to the Stackdriver
    Logging API.

    It can also be used to save logs for executing tasks. To do this, you should set as a handler with
    the name "tasks". In this case, it will also be used to read the log for display in Web UI.

    This handler supports both an asynchronous and synchronous transport.

    :param gcp_conn_id: Connection ID that will be used for authorization to the Google Cloud Platform.
        If omitted, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :type gcp_conn_id: str
    :param name: the name of the custom log in Stackdriver Logging. Defaults
        to 'airflow'. The name of the Python logger will be represented
         in the ``python_logger`` field.
    :type name: str
    :param transport: Class for creating new transport objects. It should
        extend from the base :class:`google.cloud.logging.handlers.Transport` type and
        implement :meth`google.cloud.logging.handlers.Transport.send`. Defaults to
        :class:`google.cloud.logging.handlers.BackgroundThreadTransport`. The other
        option is :class:`google.cloud.logging.handlers.SyncTransport`.
    :type transport: :class:`type`
    :param resource: (Optional) Monitored resource of the entry, defaults
                     to the global resource type.
    :type resource: :class:`~google.cloud.logging.resource.Resource`
    :param labels: (Optional) Mapping of labels for the entry.
    :type labels: dict
    """

    LABEL_TASK_ID = "task_id"
    LABEL_DAG_ID = "dag_id"
    LABEL_EXECUTION_DATE = "execution_date"
    LABEL_TRY_NUMBER = "try_number"

    def __init__(
        self,
        gcp_conn_id: Optional[str] = None,
        name: str = DEFAULT_LOGGER_NAME,
        transport: Type[Transport] = BackgroundThreadTransport,
        resource: Resource = _GLOBAL_RESOURCE,
        labels: Optional[Dict[str, str]] = None,
    ):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.name: str = name
        self.transport_type: Type[Transport] = transport
        self.resource: Resource = resource
        self.labels: Optional[Dict[str, str]] = labels
        self.task_instance_labels: Optional[Dict[str, str]] = {}

    @cached_property
    def _client(self) -> gcp_logging.Client:
        """Google Cloud Library API client"""
        if self.gcp_conn_id:
            from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

            hook = GoogleBaseHook(gcp_conn_id=self.gcp_conn_id)
            credentials = hook._get_credentials()  # pylint: disable=protected-access
        else:
            # Use Application Default Credentials
            credentials = None
        client = gcp_logging.Client(
            credentials=credentials,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version)
        )
        return client

    @cached_property
    def _transport(self) -> Transport:
        """Object responsible for sending data to Stackdriver"""
        return self.transport_type(self._client, self.name)

    def emit(self, record: logging.LogRecord) -> None:
        """Actually log the specified logging record.

        :param record: The record to be logged.
        :type record: logging.LogRecord
        """
        message = self.format(record)
        labels: Optional[Dict[str, str]]
        if self.labels and self.task_instance_labels:
            labels = {}
            labels.update(self.labels)
            labels.update(self.task_instance_labels)
        elif self.labels:
            labels = self.labels
        elif self.task_instance_labels:
            labels = self.task_instance_labels
        else:
            labels = None
        self._transport.send(record, message, resource=self.resource, labels=labels)

    def set_context(self, task_instance: TaskInstance) -> None:
        """
        Configures the logger to add information with information about the current task

        :param task_instance: Currently executed task
        :type task_instance: TaskInstance
        """
        self.task_instance_labels = self._task_instance_to_labels(task_instance)

    def read(
        self, task_instance: TaskInstance, try_number: Optional[int] = None, metadata: Optional[Dict] = None
    ) -> Tuple[List[str], List[Dict]]:
        """
        Read logs of given task instance from Stackdriver logging.

        :param task_instance: task instance object
        :type: task_instance: TaskInstance
        :param try_number: task instance try_number to read logs from. If None
           it returns all logs
        :type try_number: Optional[int]
        :param metadata: log metadata. It is used for steaming log reading and auto-tailing.
        :type metadata: Dict
        :return: a tuple of list of logs and list of metadata
        :rtype: Tuple[List[str], List[Dict]]
        """
        if try_number is not None and try_number < 1:
            logs = ["Error fetching the logs. Try number {} is invalid.".format(try_number)]
            return logs, [{"end_of_log": "true"}]

        if not metadata:
            metadata = {}

        ti_labels = self._task_instance_to_labels(task_instance)

        if try_number is not None:
            ti_labels[self.LABEL_TRY_NUMBER] = str(try_number)
        else:
            del ti_labels[self.LABEL_TRY_NUMBER]

        log_filter = self._prepare_log_filter(ti_labels)
        next_page_token = metadata.get("next_page_token", None)
        all_pages = 'download_logs' in metadata and metadata['download_logs']

        messages, end_of_log, next_page_token = self._read_logs(log_filter, next_page_token, all_pages)

        new_metadata = {"end_of_log": end_of_log}

        if next_page_token:
            new_metadata['next_page_token'] = next_page_token

        return [messages], [new_metadata]

    def _prepare_log_filter(self, ti_labels: Dict[str, str]) -> str:
        """
        Prepares the filter that chooses which log entries to fetch.

        More information:
        https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list#body.request_body.FIELDS.filter
        https://cloud.google.com/logging/docs/view/advanced-queries

        :param ti_labels: Task Instance's labels that will be used to search for logs
        :type: Dict[str, str]
        :return: logs filter
        """
        def escape_label_key(key: str) -> str:
            return f'"{key}"' if "." in key else key

        def escale_label_value(value: str) -> str:
            escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped_value}"'

        log_filters = [
            f'resource.type={escale_label_value(self.resource.type)}',
            f'logName="projects/{self._client.project}/logs/{self.name}"'
        ]

        for key, value in self.resource.labels.items():
            log_filters.append(f'resource.labels.{escape_label_key(key)}={escale_label_value(value)}')

        for key, value in ti_labels.items():
            log_filters.append(f'labels.{escape_label_key(key)}={escale_label_value(value)}')
        return "\n".join(log_filters)

    def _read_logs(
        self,
        log_filter: str,
        next_page_token: Optional[str],
        all_pages: bool
    ) -> Tuple[str, bool, Optional[str]]:
        """
        Sends requests to the Stackdriver service and downloads logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param next_page_token: The token of the page from which the log download will start.
            If None is passed, it will start from the first page.
        :param all_pages: If True is passed, all subpages will be downloaded. Otherwise, only the first
            page will be downloaded
        :return: A token that contains the following items:
            * string with logs
            * Boolean value describing whether there are more logs,
            * token of the next page
        :rtype: Tuple[str, bool, str]
        """
        messages = []
        new_messages, next_page_token = self._read_single_logs_page(
            log_filter=log_filter,
            page_token=next_page_token,
        )
        messages.append(new_messages)
        if all_pages:
            while next_page_token:
                new_messages, next_page_token = self._read_single_logs_page(
                    log_filter=log_filter,
                    page_token=next_page_token
                )
                messages.append(new_messages)

            end_of_log = True
            next_page_token = None
        else:
            end_of_log = not bool(next_page_token)
        return "\n".join(messages), end_of_log, next_page_token

    def _read_single_logs_page(self, log_filter: str, page_token: Optional[str] = None) -> Tuple[str, str]:
        """
        Sends requests to the Stackdriver service and downloads single pages with logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param page_token: The token of the page to be downloaded. If None is passed, the first page will be
            downloaded.
        :type page_token: str
        :return: Downloaded logs and next page token
        :rtype: Tuple[str, str]
        """
        entries = self._client.list_entries(filter_=log_filter, page_token=page_token)
        page = next(entries.pages)
        next_page_token = entries.next_page_token
        messages = []
        for entry in page:
            if "message" in entry.payload:
                messages.append(entry.payload["message"])

        return "\n".join(messages), next_page_token

    @classmethod
    def _task_instance_to_labels(cls, ti: TaskInstance) -> Dict[str, str]:
        return {
            cls.LABEL_TASK_ID: ti.task_id,
            cls.LABEL_DAG_ID: ti.dag_id,
            cls.LABEL_EXECUTION_DATE: str(ti.execution_date.isoformat()),
            cls.LABEL_TRY_NUMBER: str(ti.try_number),
        }

```



## 日志处理与读取接口

file_task_handler.py，用于处理和读取任务实例日志。它创建并将日志处理委托给“日志记录”。在接收任务实例上下文之后。它从任务实例的主机读取日志。

```python
import logging
import os
from typing import Optional

import requests

from airflow.configuration import AirflowConfigException, conf
from airflow.models import TaskInstance
from airflow.utils.file import mkdirs
from airflow.utils.helpers import parse_template_string


class FileTaskHandler(logging.Handler):
    """
    FileTaskHandler is a python log handler that handles and reads
    task instance logs. It creates and delegates log handling
    to `logging.FileHandler` after receiving task instance context.
    It reads logs from task instance's host machine.

    :param base_log_folder: Base log folder to place logs.
    :param filename_template: template filename string
    """
    def __init__(self, base_log_folder: str, filename_template: str):
        super().__init__()
        self.handler = None  # type: Optional[logging.FileHandler]
        self.local_base = base_log_folder
        self.filename_template, self.filename_jinja_template = \
            parse_template_string(filename_template)

    def set_context(self, ti: TaskInstance):
        """
        Provide task_instance context to airflow task handler.

        :param ti: task instance object
        """
        local_loc = self._init_file(ti)
        self.handler = logging.FileHandler(local_loc, encoding='utf-8')
        if self.formatter:
            self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)

    def emit(self, record):
        if self.handler:
            self.handler.emit(record)

    def flush(self):
        if self.handler:
            self.handler.flush()

    def close(self):
        if self.handler:
            self.handler.close()

    def _render_filename(self, ti, try_number):
        if self.filename_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return self.filename_jinja_template.render(**jinja_context)

        return self.filename_template.format(dag_id=ti.dag_id,
                                             task_id=ti.task_id,
                                             execution_date=ti.execution_date.isoformat(),
                                             try_number=try_number)

    def _read(self, ti, try_number, metadata=None):  # pylint: disable=unused-argument
        """
        Template method that contains custom logic of reading
        logs given the try_number.

        :param ti: task instance record
        :param try_number: current try_number to read log from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: log message as a string and metadata.
        """
        # Task instance here might be different from task instance when
        # initializing the handler. Thus explicitly getting log location
        # is needed to get correct log path.
        log_relative_path = self._render_filename(ti, try_number)
        location = os.path.join(self.local_base, log_relative_path)

        log = ""

        if os.path.exists(location):
            try:
                with open(location) as file:
                    log += "*** Reading local file: {}\n".format(location)
                    log += "".join(file.readlines())
            except Exception as e:  # pylint: disable=broad-except
                log = "*** Failed to load local log file: {}\n".format(location)
                log += "*** {}\n".format(str(e))
        else:
            url = os.path.join(
                "http://{ti.hostname}:{worker_log_server_port}/log", log_relative_path
            ).format(
                ti=ti,
                worker_log_server_port=conf.get('celery', 'WORKER_LOG_SERVER_PORT')
            )
            log += "*** Log file does not exist: {}\n".format(location)
            log += "*** Fetching from: {}\n".format(url)
            try:
                timeout = None  # No timeout
                try:
                    timeout = conf.getint('webserver', 'log_fetch_timeout_sec')
                except (AirflowConfigException, ValueError):
                    pass

                response = requests.get(url, timeout=timeout)
                response.encoding = "utf-8"

                # Check if the resource was properly fetched
                response.raise_for_status()

                log += '\n' + response.text
            except Exception as e:  # pylint: disable=broad-except
                log += "*** Failed to fetch log file from worker. {}\n".format(str(e))

        return log, {'end_of_log': True}

    def read(self, task_instance, try_number=None, metadata=None):
        """
        Read logs of given task instance from local machine.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                           it returns all logs separated by try_number
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: a list of logs
        """
        # Task instance increments its try number when it starts to run.
        # So the log for a particular task try will only show up when
        # try number gets incremented in DB, i.e logs produced the time
        # after cli run and before try_number + 1 in DB will not be displayed.

        if try_number is None:
            next_try = task_instance.next_try_number
            try_numbers = list(range(1, next_try))
        elif try_number < 1:
            logs = [
                'Error fetching the logs. Try number {} is invalid.'.format(try_number),
            ]
            return logs
        else:
            try_numbers = [try_number]

        logs = [''] * len(try_numbers)
        metadata_array = [{}] * len(try_numbers)
        for i, try_number_element in enumerate(try_numbers):
            log, metadata = self._read(task_instance, try_number_element, metadata)
            logs[i] += log
            metadata_array[i] = metadata

        return logs, metadata_array

    def _init_file(self, ti):
        """
        Create log directory and give it correct permissions.

        :param ti: task instance object
        :return: relative log path of the given task instance
        """
        # To handle log writing when tasks are impersonated, the log files need to
        # be writable by the user that runs the Airflow command and the user
        # that is impersonated. This is mainly to handle corner cases with the
        # SubDagOperator. When the SubDagOperator is run, all of the operators
        # run under the impersonated user and create appropriate log files
        # as the impersonated user. However, if the user manually runs tasks
        # of the SubDagOperator through the UI, then the log files are created
        # by the user that runs the Airflow command. For example, the Airflow
        # run command may be run by the `airflow_sudoable` user, but the Airflow
        # tasks may be run by the `airflow` user. If the log files are not
        # writable by both users, then it's possible that re-running a task
        # via the UI (or vice versa) results in a permission error as the task
        # tries to write to a log file created by the other user.
        relative_path = self._render_filename(ti, ti.try_number)
        full_path = os.path.join(self.local_base, relative_path)
        directory = os.path.dirname(full_path)
        # Create the log file and give it group writable permissions
        # TODO(aoen): Make log dirs and logs globally readable for now since the SubDag
        # operator is not compatible with impersonation (e.g. if a Celery executor is used
        # for a SubDag operator and the SubDag operator has a different owner than the
        # parent DAG)
        if not os.path.exists(directory):
            # Create the directory as globally writable using custom mkdirs
            # as os.makedirs doesn't set mode properly.
            mkdirs(directory, 0o777)

        if not os.path.exists(full_path):
            open(full_path, "a").close()
            # TODO: Investigate using 444 instead of 666.
            os.chmod(full_path, 0o666)

        return full_path
```

## Google Cloud Storage

gcs_task_handler.py是一个处理和读取任务实例日志的python日志处理程序。它扩展了airflow FileTaskHandler，并上传和读取GCS远程存储。在日志读取失败时，它从主机的本地磁盘进行读取。

要启用此功能，`airflow.cfg`必须按照以下示例进行配置：

```
[core]
# Airflow can store logs remotely in AWS S3, Google Cloud Storage or Elastic Search.
# Users must supply an Airflow connection id that provides access to the storage
# location. If remote_logging is set to true, see UPDATING.md for additional
# configuration requirements.
remote_logging = True
remote_base_log_folder = gs://my-bucket/path/to/logs
remote_log_conn_id = MyGCSConn
```

1. 首先安装`gcp`软件包，如下所示：。`pip install 'apache-airflow[gcp]'`
2. 确保在Airflow中定义了Google Cloud Platform连接挂钩。该钩子应该具有对上面在中定义的Google Cloud Storage存储桶的读写权限。`remote_base_log_folder`
3. 重新启动Airflow Web服务器和调度程序，并触发（或等待）新任务执行。
4. 验证是否为您定义的存储桶中新执行的任务显示了日志。
5. 验证Google Cloud Storage查看器是否在用户界面中正常工作。拉起一个新执行的任务，并确认您看到类似以下内容的内容：

```
*** Reading remote log from gs://<bucket where logs should be persisted>/example_bash_operator/run_this_last/2017-10-03T00:00:00/16.log.
[2017-10-03 21:57:50,056] {cli.py:377} INFO - Running on host chrisr-00532
[2017-10-03 21:57:50,093] {base_task_runner.py:115} INFO - Running: ['bash', '-c', u'airflow run example_bash_operator run_this_last 2017-10-03T00:00:00 --job_id 47 --raw -sd DAGS_FOLDER/example_dags/example_bash_operator.py']
[2017-10-03 21:57:51,264] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,263] {__init__.py:45} INFO - Using executor SequentialExecutor
[2017-10-03 21:57:51,306] {base_task_runner.py:98} INFO - Subtask: [2017-10-03 21:57:51,306] {models.py:186} INFO - Filling up the DagBag from /airflow/dags/example_dags/example_bash_operator.py
```

**请注意**，远程日志文件的路径在第一行中列出。

```python
import os
from urllib.parse import urlparse
from cached_property import cached_property
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class GCSTaskHandler(FileTaskHandler, LoggingMixin):
    """
    GCSTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from GCS remote storage. Upon log reading
    failure, it reads from host machine's local disk.
    """
    def __init__(self, base_log_folder, gcs_log_folder, filename_template):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = gcs_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False
        self.upload_on_close = True

    @cached_property
    def hook(self):
        """
        Returns GCS hook.
        """
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.google.cloud.hooks.gcs import GCSHook
            return GCSHook(
                google_cloud_storage_conn_id=remote_conn_id
            )
        except Exception as e:  # pylint: disable=broad-except
            self.log.error(
                'Could not create a GoogleCloudStorageHook with connection id '
                '"%s". %s\n\nPlease make sure that airflow[gcp] is installed '
                'and the GCS connection exists.', remote_conn_id, str(e)
            )

    def set_context(self, ti):
        super().set_context(ti)
        # Log relative path is used to construct local and remote
        # log path to upload log files into GCS and read from the
        # remote location.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

    def close(self):
        """
        Close and upload local log file to remote storage GCS.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.gcs_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from GCS.
        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        try:
            remote_log = self.gcs_read(remote_loc)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
            return log, {'end_of_log': True}
        except Exception as e:  # pylint: disable=broad-except
            log = '*** Unable to read remote log from {}\n*** {}\n\n'.format(
                remote_loc, str(e))
            self.log.error(log)
            local_log, metadata = super()._read(ti, try_number)
            log += local_log
            return log, metadata

    def gcs_read(self, remote_log_location):
        """
        Returns the log found at the remote_log_location.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        """
        bkt, blob = self.parse_gcs_url(remote_log_location)
        return self.hook.download(bkt, blob).decode('utf-8')

    def gcs_write(self, log, remote_log_location, append=True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: str
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append:
            try:
                old_log = self.gcs_read(remote_log_location)
                log = '\n'.join([old_log, log]) if old_log else log
            except Exception as e:  # pylint: disable=broad-except
                if not hasattr(e, 'resp') or e.resp.get('status') != '404':  # pylint: disable=no-member
                    log = '*** Previous log discarded: {}\n\n'.format(str(e)) + log

        try:
            bkt, blob = self.parse_gcs_url(remote_log_location)
            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile(mode='w+') as tmpfile:
                tmpfile.write(log)
                # Force the file to be flushed, since we're doing the
                # upload from within the file context (it hasn't been
                # closed).
                tmpfile.flush()
                self.hook.upload(bkt, blob, tmpfile.name)
        except Exception as e:  # pylint: disable=broad-except
            self.log.error('Could not write logs to %s: %s', remote_log_location, e)

    @staticmethod
    def parse_gcs_url(gsurl):
        """
        Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
        tuple containing the corresponding bucket and blob.
        """
        parsed_url = urlparse(gsurl)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket name')
        else:
            bucket = parsed_url.netloc
            blob = parsed_url.path.strip('/')
            return bucket, blob
```

## Json Formatter

json_formatter.py模块存储所有与ElasticSearch特定的日志记录器类相关的内容

JSONFormatter实例用于将日志记录转换为json。

```python
"""
json_formatter module stores all related to ElasticSearch specific logger classes
"""

import json
import logging

from airflow.utils.helpers import merge_dicts


class JSONFormatter(logging.Formatter):
    """
    JSONFormatter instances are used to convert a log record to json.
    """
    # pylint: disable=too-many-arguments
    def __init__(self, fmt=None, datefmt=None, style='%', json_fields=None, extras=None):
        super().__init__(fmt, datefmt, style)
        if extras is None:
            extras = {}
        if json_fields is None:
            json_fields = []
        self.json_fields = json_fields
        self.extras = extras

    def format(self, record):
        super().format(record)
        record_dict = {label: getattr(record, label, None)
                       for label in self.json_fields}
        merged_record = merge_dicts(record_dict, self.extras)
        return json.dumps(merged_record)

```



## Cloudwatch

cloudwatch_task_handler是一个处理和读取任务实例日志的python日志处理程序。它扩展了aifflow FileTaskHandler并上传和读取Cloudwatch。

```python
import watchtower
from cached_property import cached_property

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class CloudwatchTaskHandler(FileTaskHandler, LoggingMixin):
    """
    CloudwatchTaskHandler is a python log handler that handles and reads task instance logs.

    It extends airflow FileTaskHandler and uploads to and reads from Cloudwatch.

    :param base_log_folder: base folder to store logs locally
    :type base_log_folder: str
    :param log_group_arn: ARN of the Cloudwatch log group for remote log storage
        with format ``arn:aws:logs:{region name}:{account id}:log-group:{group name}``
    :type log_group_arn: str
    :param filename_template: template for file name (local storage) or log stream name (remote)
    :type filename_template: str
    """

    def __init__(self, base_log_folder, log_group_arn, filename_template):
        super().__init__(base_log_folder, filename_template)
        split_arn = log_group_arn.split(':')

        self.handler = None
        self.log_group = split_arn[6]
        self.region_name = split_arn[3]
        self.closed = False

    @cached_property
    def hook(self):
        """
        Returns AwsLogsHook.
        """
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
            return AwsLogsHook(aws_conn_id=remote_conn_id, region_name=self.region_name)
        except Exception:  # pylint: disable=broad-except
            self.log.error(
                'Could not create an AwsLogsHook with connection id "%s". '
                'Please make sure that airflow[aws] is installed and '
                'the Cloudwatch logs connection exists.', remote_conn_id
            )

    def _render_filename(self, ti, try_number):
        # Replace unsupported log group name characters
        return super()._render_filename(ti, try_number).replace(':', '_')

    def set_context(self, ti):
        super().set_context(ti)
        self.handler = watchtower.CloudWatchLogHandler(
            log_group=self.log_group,
            stream_name=self._render_filename(ti, ti.try_number),
            boto3_session=self.hook.get_session(self.region_name)
        )

    def close(self):
        """
        Close the handler responsible for the upload of the local log file to Cloudwatch.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if self.handler is not None:
            self.handler.close()
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, task_instance, try_number, metadata=None):
        stream_name = self._render_filename(task_instance, try_number)
        return '*** Reading remote log from Cloudwatch log_group: {} log_stream: {}.\n{}\n'.format(
            self.log_group, stream_name, self.get_cloudwatch_logs(stream_name=stream_name)
        ), {'end_of_log': True}

    def get_cloudwatch_logs(self, stream_name):
        """
        Return all logs from the given log stream.

        :param stream_name: name of the Cloudwatch log stream to get all logs from
        :return: string of all logs from the given log stream
        """
        try:
            events = list(self.hook.get_log_events(log_group=self.log_group, log_stream_name=stream_name))
            return '\n'.join(reversed([event['message'] for event in events]))
        except Exception:  # pylint: disable=broad-except
            msg = 'Could not read remote logs from log_group: {} log_stream: {}.'.format(
                self.log_group, stream_name
            )
            self.log.exception(msg)
            return msg
```

## Wasb

wasb_task_handler.py是一个python日志处理程序，用于处理和读取任务实例日志。它扩展了风流FileTaskHandler，并向Wasb远程存储上传和读取数据。

```python
import os
import shutil

from azure.common import AzureHttpError
from cached_property import cached_property

from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class WasbTaskHandler(FileTaskHandler, LoggingMixin):
    """
    WasbTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from Wasb remote storage.
    """

    def __init__(self, base_log_folder, wasb_log_folder, wasb_container,
                 filename_template, delete_local_copy):
        super().__init__(base_log_folder, filename_template)
        self.wasb_container = wasb_container
        self.remote_base = wasb_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False
        self.upload_on_close = True
        self.delete_local_copy = delete_local_copy

    @cached_property
    def hook(self):
        """
        Returns WasbHook.
        """
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
            return WasbHook(remote_conn_id)
        except AzureHttpError:
            self.log.error(
                'Could not create an WasbHook with connection id "%s". '
                'Please make sure that airflow[azure] is installed and '
                'the Wasb connection exists.', remote_conn_id
            )

    def set_context(self, ti):
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to Wasb remote storage.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

    def close(self):
        """
        Close and upload local log file to remote storage Wasb.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.wasb_write(log, remote_loc, append=True)

            if self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from Wasb remote storage.
        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        if self.wasb_log_exists(remote_loc):
            # If Wasb remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # returned remote_log will contain error messages.
            remote_log = self.wasb_read(remote_loc, return_error=True)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
            return log, {'end_of_log': True}
        else:
            return super()._read(ti, try_number)

    def wasb_log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage

        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        try:
            return self.hook.check_for_blob(self.wasb_container, remote_log_location)
        except Exception:  # pylint: disable=broad-except
            pass
        return False

    def wasb_read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        try:
            return self.hook.read_file(self.wasb_container, remote_log_location)
        except AzureHttpError:
            msg = 'Could not read logs from {}'.format(remote_log_location)
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg

    def wasb_write(self, log, remote_log_location, append=True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: str
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append and self.wasb_log_exists(remote_log_location):
            old_log = self.wasb_read(remote_log_location)
            log = '\n'.join([old_log, log]) if old_log else log

        try:
            self.hook.load_string(
                log,
                self.wasb_container,
                remote_log_location,
            )
        except AzureHttpError:
            self.log.exception('Could not write logs to %s',
                               remote_log_location)

```

## Amazon S3

s3_task_handler是一个python日志处理程序，用于处理和读取任务实例日志。它扩展了airflow FileTaskHandler并上传和读取S3远程存储。

```python
import os
from cached_property import cached_property
from airflow.configuration import conf
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class S3TaskHandler(FileTaskHandler, LoggingMixin):
    """
    S3TaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from S3 remote storage.
    """
    def __init__(self, base_log_folder, s3_log_folder, filename_template):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = s3_log_folder
        self.log_relative_path = ''
        self._hook = None
        self.closed = False
        self.upload_on_close = True

    @cached_property
    def hook(self):
        """
        Returns S3Hook.
        """
        remote_conn_id = conf.get('logging', 'REMOTE_LOG_CONN_ID')
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            return S3Hook(remote_conn_id)
        except Exception:  # pylint: disable=broad-except
            self.log.error(
                'Could not create an S3Hook with connection id "%s". '
                'Please make sure that airflow[aws] is installed and '
                'the S3 connection exists.', remote_conn_id
            )

    def set_context(self, ti):
        super().set_context(ti)
        # Local location and remote location is needed to open and
        # upload local log file to S3 remote storage.
        self.log_relative_path = self._render_filename(ti, ti.try_number)
        self.upload_on_close = not ti.raw

        # Clear the file first so that duplicate data is not uploaded
        # when re-using the same path (e.g. with rescheduled sensors)
        if self.upload_on_close:
            with open(self.handler.baseFilename, 'w'):
                pass

    def close(self):
        """
        Close and upload local log file to remote storage S3.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc, 'r') as logfile:
                log = logfile.read()
            self.s3_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from S3 remote storage.
        If failed, read the log from task instance host machine.

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, log_relative_path)

        if self.s3_log_exists(remote_loc):
            # If S3 remote file exists, we do not fetch logs from task instance
            # local machine even if there are errors reading remote logs, as
            # returned remote_log will contain error messages.
            remote_log = self.s3_read(remote_loc, return_error=True)
            log = '*** Reading remote log from {}.\n{}\n'.format(
                remote_loc, remote_log)
            return log, {'end_of_log': True}
        else:
            return super()._read(ti, try_number)

    def s3_log_exists(self, remote_log_location):
        """
        Check if remote_log_location exists in remote storage

        :param remote_log_location: log's location in remote storage
        :return: True if location exists else False
        """
        try:
            return self.hook.get_key(remote_log_location) is not None
        except Exception:  # pylint: disable=broad-except
            pass
        return False

    def s3_read(self, remote_log_location, return_error=False):
        """
        Returns the log found at the remote_log_location. Returns '' if no
        logs are found or there is an error.

        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param return_error: if True, returns a string error message if an
            error occurs. Otherwise returns '' when an error occurs.
        :type return_error: bool
        """
        try:
            return self.hook.read_key(remote_log_location)
        except Exception:  # pylint: disable=broad-except
            msg = 'Could not read logs from {}'.format(remote_log_location)
            self.log.exception(msg)
            # return error if needed
            if return_error:
                return msg

    def s3_write(self, log, remote_log_location, append=True):
        """
        Writes the log to the remote_log_location. Fails silently if no hook
        was created.

        :param log: the log to write to the remote_log_location
        :type log: str
        :param remote_log_location: the log's location in remote storage
        :type remote_log_location: str (path)
        :param append: if False, any existing log file is overwritten. If True,
            the new log is appended to any existing logs.
        :type append: bool
        """
        if append and self.s3_log_exists(remote_log_location):
            old_log = self.s3_read(remote_log_location)
            log = '\n'.join([old_log, log]) if old_log else log

        try:
            self.hook.load_string(
                log,
                key=remote_log_location,
                replace=True,
                encrypt=conf.getboolean('logging', 'ENCRYPT_S3_LOGS'),
            )
        except Exception:  # pylint: disable=broad-except
            self.log.exception('Could not write logs to %s', remote_log_location)

```

## 日志级别颜色

负责根据日志级别给日志上色。

自定义日志格式化程序，扩展'有色'。通过将属性添加到消息参数和着色错误回溯来创建TTYColoredFormatter。

```python
"""
Class responsible for colouring logs based on log level.
"""
import re
import sys
from logging import LogRecord
from typing import Any, Union

from colorlog import TTYColoredFormatter
from termcolor import colored

ARGS = {"attrs": ["bold"]}

DEFAULT_COLORS = {
    "DEBUG": "red",
    "INFO": "",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red",
}


class CustomTTYColoredFormatter(TTYColoredFormatter):
    """
    Custom log formatter which extends `colored.TTYColoredFormatter`
    by adding attributes to message arguments and coloring error
    traceback.
    """
    def __init__(self, *args, **kwargs):
        kwargs["stream"] = sys.stdout or kwargs.get("stream")
        kwargs["log_colors"] = DEFAULT_COLORS
        super().__init__(*args, **kwargs)

    @staticmethod
    def _color_arg(arg: Any) -> Union[str, float, int]:
        if isinstance(arg, (int, float)):
            # In case of %d or %f formatting
            return arg
        return colored(str(arg), **ARGS)  # type: ignore

    @staticmethod
    def _count_number_of_arguments_in_message(record: LogRecord) -> int:
        matches = re.findall(r"%.", record.msg)
        return len(matches) if matches else 0

    def _color_record_args(self, record: LogRecord) -> LogRecord:
        if isinstance(record.args, (tuple, list)):
            record.args = tuple(self._color_arg(arg) for arg in record.args)
        elif isinstance(record.args, dict):
            if self._count_number_of_arguments_in_message(record) > 1:
                # Case of logging.debug("a %(a)d b %(b)s", {'a':1, 'b':2})
                record.args = {
                    key: self._color_arg(value) for key, value in record.args.items()
                }
            else:
                # Case of single dict passed to formatted string
                record.args = self._color_arg(record.args)   # type: ignore
        elif isinstance(record.args, str):
            record.args = self._color_arg(record.args)
        return record

    def _color_record_traceback(self, record: LogRecord) -> LogRecord:
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

            if record.exc_text:
                record.exc_text = colored(record.exc_text, DEFAULT_COLORS["ERROR"])
        return record

    def format(self, record: LogRecord) -> str:
        try:
            record = self._color_record_args(record)
            record = self._color_record_traceback(record)
            return super().format(record)
        except ValueError:  # I/O operation on closed file
            from logging import Formatter
            return Formatter().format(record)
```

## Logging Mixin

```python
import logging
import re
import sys
from logging import Handler, Logger, StreamHandler

# 7-bit C1 ANSI escape sequences
ANSI_ESCAPE = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')


def remove_escape_codes(text: str) -> str:
    """
    Remove ANSI escapes codes from string. It's used to remove
    "colors" from log messages.
    """
    return ANSI_ESCAPE.sub("", text)


class LoggingMixin:
    """
    Convenience super-class to have a logger configured with the class name
    """
    def __init__(self, context=None):
        self._set_context(context)

    @property
    def log(self) -> Logger:
        """
        Returns a logger.
        """
        try:
            # FIXME: LoggingMixin should have a default _log field.
            return self._log  # type: ignore
        except AttributeError:
            self._log = logging.getLogger(
                self.__class__.__module__ + '.' + self.__class__.__name__
            )
            return self._log

    def _set_context(self, context):
        if context is not None:
            set_context(self.log, context)


# TODO: Formally inherit from io.IOBase
class StreamLogWriter:
    """
    Allows to redirect stdout and stderr to logger
    """
    encoding: None = None

    def __init__(self, logger, level):
        """
        :param log: The log level method to write to, ie. log.debug, log.warning
        :return:
        """
        self.logger = logger
        self.level = level
        self._buffer = str()

    @property
    def closed(self):
        """
        Returns False to indicate that the stream is not closed (as it will be
        open for the duration of Airflow's lifecycle).

        For compatibility with the io.IOBase interface.
        """
        return False

    def _propagate_log(self, message):
        """
        Propagate message removing escape codes.
        """
        self.logger.log(self.level, remove_escape_codes(message))

    def write(self, message):
        """
        Do whatever it takes to actually log the specified logging record

        :param message: message to log
        """
        if not message.endswith("\n"):
            self._buffer += message
        else:
            self._buffer += message
            self._propagate_log(self._buffer.rstrip())
            self._buffer = str()

    def flush(self):
        """
        Ensure all logging output has been flushed
        """
        if len(self._buffer) > 0:
            self._propagate_log(self._buffer)
            self._buffer = str()

    def isatty(self):
        """
        Returns False to indicate the fd is not connected to a tty(-like) device.
        For compatibility reasons.
        """
        return False


class RedirectStdHandler(StreamHandler):
    """
    This class is like a StreamHandler using sys.stderr/stdout, but always uses
    whatever sys.stderr/stderr is currently set to rather than the value of
    sys.stderr/stdout at handler construction time.
    """
    # pylint: disable=super-init-not-called
    def __init__(self, stream):
        if not isinstance(stream, str):
            raise Exception("Cannot use file like objects. Use 'stdout' or 'stderr'"
                            " as a str and without 'ext://'.")

        self._use_stderr = True
        if 'stdout' in stream:
            self._use_stderr = False

        # StreamHandler tries to set self.stream
        Handler.__init__(self)  # pylint: disable=non-parent-init-called

    @property
    def stream(self):
        """
        Returns current stream.
        """
        if self._use_stderr:
            return sys.stderr

        return sys.stdout


def set_context(logger, value):
    """
    Walks the tree of loggers and tries to set the context for each handler

    :param logger: logger
    :param value: value to set
    """
    _logger = logger
    while _logger:
        for handler in _logger.handlers:
            try:
                handler.set_context(value)
            except AttributeError:
                # Not all handlers need to have context passed in so we ignore
                # the error when handlers do not have set_context defined.
                pass
        if _logger.propagate is True:
            _logger = _logger.parent
        else:
            _logger = None
```

