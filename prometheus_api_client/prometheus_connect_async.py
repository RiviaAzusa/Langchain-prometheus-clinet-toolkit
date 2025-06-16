"""A Class for collection of metrics from a Prometheus Host."""

from urllib.parse import urlparse
import bz2
import os
import json
import logging
from datetime import datetime, timedelta
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
import asyncio

PrometheusApiClientException = ConnectionError


# set up logging

_LOGGER = logging.getLogger(__name__)

# In case of a connection failure try 2 more times
MAX_REQUEST_RETRIES = 0
# wait 1 second before retrying in case of an error
RETRY_BACKOFF_FACTOR = 1
# retry only on these status
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]


class PrometheusConnectAsync:
    """
    A Class for collection of metrics from a Prometheus Host.

    :param url: (str) url for the prometheus host
    :param headers: (dict) A dictionary of http headers to be used to communicate with
        the host. Example: {"Authorization": "bearer my_oauth_token_to_the_host"}
    :param disable_ssl: (bool) If set to True, will disable ssl certificate verification
        for the http requests made to the prometheus host
    :param auth: (optional) Auth tuple to enable Basic/Digest/Custom HTTP Auth. See python
        aiohttp library auth parameter for further explanation.
    :param proxy: (Optional) Proxies dictionary to enable connection through proxy.
        Example: {"http_proxy": "<ip_address/hostname:port>", "https_proxy": "<ip_address/hostname:port>"}
    :param session (Optional) Custom aiohttp.ClientSession to enable complex HTTP configuration
    :param timeout: (Optional) A timeout (in seconds) applied to all requests
    """

    def __init__(
        self,
        url: str = "http://127.0.0.1:9090",
        headers: dict = None,
        disable_ssl: bool = False,
        auth: aiohttp.BasicAuth = None,
        proxy: str = None,
        session: aiohttp.ClientSession = None,
        timeout: int = None,
        max_retries: int = MAX_REQUEST_RETRIES,
    ):
        """Functions as a Constructor for the class PrometheusConnectAsync."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self.prometheus_host = urlparse(self.url).netloc
        self._all_metrics = None
        self._timeout = ClientTimeout(total=timeout) if timeout else None
        self.auth = auth
        self.proxy = proxy
        self.disable_ssl = disable_ssl
        self.max_retries = max_retries
        self._session = session
        self._should_close_session = False

    async def __aenter__(self):
        """Async enter context manager."""
        if self._session is None:
            self._session = await self._create_session()
            self._should_close_session = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async exit context manager."""
        if self._session is not None and self._should_close_session:
            await self._session.close()
            self._session = None
            self._should_close_session = False

    async def _create_session(self):
        """Create an aiohttp client session."""
        connector = TCPConnector(ssl=not self.disable_ssl)
        return aiohttp.ClientSession(
            headers=self.headers,
            auth=self.auth,
            connector=connector,
            timeout=self._timeout,
        )

    async def _get_session(self):
        """Get or create an aiohttp client session."""
        if self._session is None:
            self._session = await self._create_session()
            self._should_close_session = True
        return self._session

    async def _do_request(self, endpoint, method="GET", **kwargs):
        """Perform HTTP request with retry logic."""
        session = await self._get_session()

        for attempt in range(self.max_retries + 1):
            try:
                async with session.request(
                    method=method,
                    url=f"{self.url}{endpoint}",
                    proxy=self.proxy,
                    **kwargs,
                ) as response:
                    if response.status >= 400:
                        error_message = f"HTTP Status Code {response.status} ({await response.text()})"
                        raise PrometheusApiClientException(error_message)

                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if (
                    attempt < self.max_retries
                    and isinstance(e, aiohttp.ServerTimeoutError)
                    or (hasattr(e, "status") and e.status in RETRY_ON_STATUS)
                ):
                    # Wait with exponential backoff before retrying
                    await asyncio.sleep(RETRY_BACKOFF_FACTOR * (2**attempt))
                    continue
                raise PrometheusApiClientException(
                    f"Request failed after {attempt + 1} attempts: {str(e)}"
                )

    async def check_prometheus_connection(self, params: dict = None) -> bool:
        """
        Check Prometheus connection.

        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        session = await self._get_session()
        try:
            async with session.get(
                f"{self.url}/",
                params=params,
                proxy=self.proxy,
            ) as response:
                return response.ok
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return False

    async def all_metrics(self, params: dict = None):
        """
        Get the list of all the metrics that the prometheus host scrapes.

        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of names of all the metrics available from the
            specified prometheus host
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        self._all_metrics = await self.get_label_values(
            label_name="__name__", params=params
        )
        return self._all_metrics

    async def get_label_names(self, params: dict = None):
        """
        Get a list of all labels.

        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "start", "end" or "match[]".
        :returns: (list) A list of labels from the specified prometheus host
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        data = await self._do_request("/api/v1/labels", params=params)
        return data["data"]

    async def get_label_values(self, label_name: str, params: dict = None):
        """
        Get a list of all values for the label.

        :param label_name: (str) The name of the label for which you want to get all the values.
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :returns: (list) A list of names for the label from the specified prometheus host
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        data = await self._do_request(
            f"/api/v1/label/{label_name}/values", params=params
        )
        return data["data"]

    async def get_current_metric_value(
        self, metric_name: str, label_config: dict = None, params: dict = None
    ):
        """
        Get the current metric value for the specified metric and label configuration.

        :param metric_name: (str) The name of the metric
        :param label_config: (dict) A dictionary that specifies metric labels and their
            values
        :param params: (dict) Optional dictionary containing GET parameters to be sent
            along with the API request, such as "time"
        :returns: (list) A list of current metric values for the specified metric
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}

        if label_config:
            label_list = [
                str(key + "=" + "'" + label_config[key] + "'") for key in label_config
            ]
            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name

        # Using the query API to get raw data
        data = await self._do_request(
            "/api/v1/query", params={**{"query": query}, **params}
        )

        return data["data"]["result"]

    async def get_metric_range_data(
        self,
        metric_name: str,
        label_config: dict = None,
        start_time: datetime = (datetime.now() - timedelta(minutes=10)),
        end_time: datetime = datetime.now(),
        chunk_size: timedelta = None,
        store_locally: bool = False,
        params: dict = None,
    ):
        """
        Get the metric value for the specified metric and label configuration over a range of time.

        :param metric_name: (str) The name of the metric.
        :param label_config: (dict) A dictionary specifying metric labels and their
            values.
        :param start_time:  (datetime) A datetime object that specifies the metric range start time.
        :param end_time: (datetime) A datetime object that specifies the metric range end time.
        :param chunk_size: (timedelta) Duration of metric data downloaded in one request.
        :param store_locally: (bool) If set to True, will store data locally
        :param params: (dict) Optional dictionary containing GET parameters
        :return: (list) A list of metric data for the specified metric in the given time range
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        data = []

        _LOGGER.debug("start_time: %s", start_time)
        _LOGGER.debug("end_time: %s", end_time)
        _LOGGER.debug("chunk_size: %s", chunk_size)

        if not (isinstance(start_time, datetime) and isinstance(end_time, datetime)):
            raise TypeError(
                "start_time and end_time can only be of type datetime.datetime"
            )

        if not chunk_size:
            chunk_size = end_time - start_time
        if not isinstance(chunk_size, timedelta):
            raise TypeError("chunk_size can only be of type datetime.timedelta")

        start = round(start_time.timestamp())
        end = round(end_time.timestamp())

        if end_time < start_time:
            raise ValueError("end_time must not be before start_time")

        if (end_time - start_time).total_seconds() < chunk_size.total_seconds():
            raise ValueError("specified chunk_size is too big")

        chunk_seconds = round(chunk_size.total_seconds())

        if label_config:
            label_list = [
                str(key + "=" + "'" + label_config[key] + "'") for key in label_config
            ]
            query = metric_name + "{" + ",".join(label_list) + "}"
        else:
            query = metric_name

        _LOGGER.debug("Prometheus Query: %s", query)

        while start < end:
            if start + chunk_seconds > end:
                chunk_seconds = end - start

            # Using the query API to get raw data
            response_data = await self._do_request(
                "/api/v1/query",
                params={
                    **{
                        "query": query + "[" + str(chunk_seconds) + "s" + "]",
                        "time": start + chunk_seconds,
                    },
                    **params,
                },
            )

            data.extend(response_data["data"]["result"])

            if store_locally:
                # Store it locally
                await self._store_metric_values_local(
                    metric_name,
                    json.dumps(response_data["data"]["result"]),
                    start + chunk_seconds,
                )

            start += chunk_seconds

        return data

    async def _store_metric_values_local(
        self, metric_name, values, end_timestamp, compressed=False
    ):
        """
        Store metrics on the local filesystem, optionally with bz2 compression.

        :param metric_name: (str) the name of the metric being saved
        :param values: (str) metric data in JSON string format
        :param end_timestamp: (int) timestamp in any format understood by datetime.fromtimestamp()
        :param compressed: (bool) whether or not to apply bz2 compression
        :returns: (str) path to the saved metric file
        """
        if not values:
            _LOGGER.debug("No values for %s", metric_name)
            return None

        file_path = self._metric_filename(metric_name, end_timestamp)

        if compressed:
            payload = bz2.compress(str(values).encode("utf-8"))
            file_path = file_path + ".bz2"
        else:
            payload = str(values).encode("utf-8")

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Using asyncio to run file operations in executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: self._write_file(file_path, payload))

        return file_path

    def _write_file(self, file_path, payload):
        """Write payload to file."""
        with open(file_path, "wb") as file:
            file.write(payload)

    def _metric_filename(self, metric_name: str, end_timestamp: int):
        """
        Add a timestamp to the filename before it is stored.

        :param metric_name: (str) the name of the metric being saved
        :param end_timestamp: (int) timestamp in any format understood by datetime.fromtimestamp()
        :returns: (str) the generated path
        """
        end_time_stamp = datetime.fromtimestamp(end_timestamp)
        directory_name = end_time_stamp.strftime("%Y%m%d")
        timestamp = end_time_stamp.strftime("%Y%m%d%H%M")
        object_path = (
            "./metrics/"
            + self.prometheus_host
            + "/"
            + metric_name
            + "/"
            + directory_name
            + "/"
            + timestamp
            + ".json"
        )
        return object_path

    async def custom_query(self, query: str, params: dict = None, timeout: int = None):
        """
        Send a custom query to a Prometheus Host.

        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.

        :param query: (str) This is a PromQL query
        :param params: (dict) Optional dictionary containing GET parameters
        :param timeout: (Optional) A timeout (in seconds) applied to the request
        :returns: (list) A list of metric data received in response of the query sent
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}

        # Override timeout for this request if specified
        request_timeout = ClientTimeout(total=timeout) if timeout else self._timeout

        session = await self._get_session()

        # Need custom logic for this request since we're using a different timeout
        for attempt in range(self.max_retries + 1):
            try:
                async with session.get(
                    f"{self.url}/api/v1/query",
                    params={**{"query": query}, **params},
                    proxy=self.proxy,
                    timeout=request_timeout,
                ) as response:
                    if response.status >= 400:
                        error_message = f"HTTP Status Code {response.status} ({await response.text()})"
                        raise PrometheusApiClientException(error_message)

                    response_data = await response.json()
                    return response_data["data"]["result"]
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if (
                    attempt < self.max_retries
                    and isinstance(e, aiohttp.ServerTimeoutError)
                    or (hasattr(e, "status") and e.status in RETRY_ON_STATUS)
                ):
                    await asyncio.sleep(RETRY_BACKOFF_FACTOR * (2**attempt))
                    continue
                raise PrometheusApiClientException(
                    f"Request failed after {attempt + 1} attempts: {str(e)}"
                )

    async def custom_query_range(
        self,
        query: str,
        start_time: datetime,
        end_time: datetime,
        step: str,
        params: dict = None,
        timeout: int = None,
    ):
        """
        Send a query_range to a Prometheus Host.

        :param query: (str) This is a PromQL query
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters
        :param timeout: (Optional) A timeout (in seconds) applied to the request
        :returns: (dict) A dict of metric data received in response of the query sent
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        start = round(start_time.timestamp())
        end = round(end_time.timestamp())
        params = params or {}

        # Using the query_range API to get raw data
        custom_params = {
            **{"query": query, "start": start, "end": end, "step": step},
            **params,
        }

        # Override timeout for this request if specified
        request_timeout = ClientTimeout(total=timeout) if timeout else self._timeout

        session = await self._get_session()

        # Need custom logic for this request since we're using a different timeout
        for attempt in range(self.max_retries + 1):
            try:
                async with session.get(
                    f"{self.url}/api/v1/query_range",
                    params=custom_params,
                    proxy=self.proxy,
                    timeout=request_timeout,
                ) as response:
                    if response.status >= 400:
                        error_message = f"HTTP Status Code {response.status} ({await response.text()})"
                        raise PrometheusApiClientException(error_message)

                    response_data = await response.json()
                    return response_data["data"]["result"]
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if (
                    attempt < self.max_retries
                    and isinstance(e, aiohttp.ServerTimeoutError)
                    or (hasattr(e, "status") and e.status in RETRY_ON_STATUS)
                ):
                    await asyncio.sleep(RETRY_BACKOFF_FACTOR * (2**attempt))
                    continue
                raise PrometheusApiClientException(
                    f"Request failed after {attempt + 1} attempts: {str(e)}"
                )

    async def get_metric_aggregation(
        self,
        query: str,
        operations: list,
        start_time: datetime = None,
        end_time: datetime = None,
        step: str = "15",
        params: dict = None,
    ):
        """
        Get aggregations on metric values received from PromQL query.

        :param query: (str) This is a PromQL query
        :param operations: (list) A list of operations to perform on the values.
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters
        :returns: (dict) A dict of aggregated values received in response to the operations
        """
        try:
            import numpy
        except ImportError:
            raise ImportError(
                "The numpy package is required for metric aggregation. "
                "Please install it using 'pip install numpy'."
            )

        if not isinstance(operations, list):
            raise TypeError("Operations can be only of type list")
        if len(operations) == 0:
            _LOGGER.debug("No operations found to perform")
            return None

        aggregated_values = {}
        query_values = []

        if start_time is not None and end_time is not None:
            data = await self.custom_query_range(
                query=query,
                params=params,
                start_time=start_time,
                end_time=end_time,
                step=step,
            )
            for result in data:
                values = result["values"]
                for val in values:
                    query_values.append(float(val[1]))
        else:
            data = await self.custom_query(query, params)
            for result in data:
                val = float(result["value"][1])
                query_values.append(val)

        if len(query_values) == 0:
            _LOGGER.debug("No values found for given query.")
            return None

        np_array = numpy.array(query_values)
        for operation in operations:
            if operation == "sum":
                aggregated_values["sum"] = numpy.sum(np_array)
            elif operation == "max":
                aggregated_values["max"] = numpy.max(np_array)
            elif operation == "min":
                aggregated_values["min"] = numpy.min(np_array)
            elif operation == "average":
                aggregated_values["average"] = numpy.average(np_array)
            elif operation.startswith("percentile"):
                try:
                    percentile = float(operation.split("_")[1])
                except (IndexError, ValueError):
                    raise TypeError(f"Invalid percentile operation format: {operation}")
                aggregated_values["percentile_" + str(percentile)] = numpy.percentile(
                    query_values, percentile
                )
            elif operation == "deviation":
                aggregated_values["deviation"] = numpy.std(np_array)
            elif operation == "variance":
                aggregated_values["variance"] = numpy.var(np_array)
            else:
                raise TypeError("Invalid operation: " + operation)

        return aggregated_values

    async def get_scrape_pools(self) -> list[str]:
        """
        Get a list of all scrape pools in activeTargets.
        """
        targets = await self.get_targets()
        scrape_pools = []
        for target in targets["activeTargets"]:
            scrape_pools.append(target["scrapePool"])
        return list(set(scrape_pools))

    async def get_targets(self, state: str = None, scrape_pool: str = None):
        """
        Get a list of all targets from Prometheus.

        :param state: (str) Optional filter for target state ('active', 'dropped', 'any').
                     If None, returns both active and dropped targets.
        :param scrape_pool: (str) Optional filter by scrape pool name
        :returns: (dict) A dictionary containing active and dropped targets
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = {}
        if state:
            params["state"] = state
        if scrape_pool:
            params["scrapePool"] = scrape_pool

        data = await self._do_request("/api/v1/targets", params=params)
        return data["data"]

    async def get_target_metadata(self, target: dict[str, str], metric: str = None):
        """
        Get metadata about metrics from a specific target.

        :param target: (dict) A dictionary containing target labels to match against
        :param metric: (str) Optional metric name to filter metadata
        :returns: (list) A list of metadata entries for matching targets
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = {}

        # Convert target dict to label selector string
        if metric:
            params["metric"] = metric

        if target:
            match_target = "{" + ",".join(f'{k}="{v}"' for k, v in target.items()) + "}"
            params["match_target"] = match_target

        data = await self._do_request("/api/v1/targets/metadata", params=params)
        return data["data"]

    async def get_metric_metadata(
        self, metric: str = None, limit: int = None, limit_per_metric: int = None
    ):
        """
        Get metadata about metrics.

        :param metric: (str) Optional metric name to filter metadata
        :param limit: (int) Optional maximum number of metrics to return
        :param limit_per_metric: (int) Optional maximum number of metadata entries per metric
        :returns: (list) A list of metadata entries
        :raises:
            (ClientError) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = {}

        if metric:
            params["metric"] = metric

        if limit:
            params["limit"] = limit

        if limit_per_metric:
            params["limit_per_metric"] = limit_per_metric

        data = await self._do_request("/api/v1/metadata", params=params)

        formatted_data = []
        for k, v in data["data"].items():
            for v_ in v:
                formatted_data.append(
                    {
                        "metric_name": k,
                        "type": v_.get("type", "unknown"),
                        "help": v_.get("help", ""),
                        "unit": v_.get("unit", ""),
                    }
                )
        return formatted_data

    async def close(self):
        """Close the aiohttp session."""
        if self._session is not None and self._should_close_session:
            await self._session.close()
            self._session = None
            self._should_close_session = False
