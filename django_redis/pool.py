import threading
from copy import deepcopy
from typing import Dict
from urllib.parse import parse_qs, urlparse

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from redis import Redis
from redis.cluster import RedisCluster
from redis.connection import ConnectionPool, DefaultParser, to_bool
from redis.sentinel import Sentinel


class ConnectionFactory:
    # Store connection pool by cache backend options.
    #
    # _pools is a process-global, as otherwise _pools is cleared every time
    # ConnectionFactory is instantiated, as Django creates new cache client
    # (DefaultClient) instance for every request.

    _pools: Dict[str, ConnectionPool] = {}

    def __init__(self, options):
        pool_cls_path = options.get(
            "CONNECTION_POOL_CLASS", "redis.connection.ConnectionPool"
        )
        self.pool_cls = import_string(pool_cls_path)
        self.pool_cls_kwargs = options.get("CONNECTION_POOL_KWARGS", {})

        redis_client_cls_path = options.get("REDIS_CLIENT_CLASS", "redis.client.Redis")
        self.redis_client_cls = import_string(redis_client_cls_path)
        self.redis_client_cls_kwargs = options.get("REDIS_CLIENT_KWARGS", {})

        self.options = options

    def make_connection_params(self, url):
        """
        Given a main connection parameters, build a complete
        dict of connection parameters.
        """

        kwargs = {
            "url": url,
            "parser_class": self.get_parser_cls(),
        }

        password = self.options.get("PASSWORD", None)
        if password:
            kwargs["password"] = password

        socket_timeout = self.options.get("SOCKET_TIMEOUT", None)
        if socket_timeout:
            if not isinstance(socket_timeout, (int, float)):
                error_message = "Socket timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwargs["socket_timeout"] = socket_timeout

        socket_connect_timeout = self.options.get("SOCKET_CONNECT_TIMEOUT", None)
        if socket_connect_timeout:
            if not isinstance(socket_connect_timeout, (int, float)):
                error_message = "Socket connect timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwargs["socket_connect_timeout"] = socket_connect_timeout

        return kwargs

    def connect(self, url: str) -> Redis:
        """
        Given a basic connection parameters,
        return a new connection.
        """
        params = self.make_connection_params(url)
        return self.get_connection(params)

    def disconnect(self, connection: Redis) -> None:
        """
        Given a not null client connection it disconnect from the Redis server.

        The default implementation uses a pool to hold connections.
        """
        connection.connection_pool.disconnect()

    def get_connection(self, params):
        """
        Given a now preformatted params, return a
        new connection.

        The default implementation uses a cached pools
        for create new connection.
        """
        pool = self.get_or_create_connection_pool(params)
        return self.redis_client_cls(
            connection_pool=pool, **self.redis_client_cls_kwargs
        )

    def get_parser_cls(self):
        cls = self.options.get("PARSER_CLASS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)

    def get_or_create_connection_pool(self, params):
        """
        Given a connection parameters and return a new
        or cached connection pool for them.

        Reimplement this method if you want distinct
        connection pool instance caching behavior.
        """
        key = params["url"]
        if key not in self._pools:
            self._pools[key] = self.get_connection_pool(params)
        return self._pools[key]

    def get_connection_pool(self, params):
        """
        Given a connection parameters, return a new
        connection pool for them.

        Overwrite this method if you want a custom
        behavior on creating connection pool.
        """
        cp_params = dict(params)
        cp_params.update(self.pool_cls_kwargs)
        pool = self.pool_cls.from_url(**cp_params)

        if pool.connection_kwargs.get("password", None) is None:
            pool.connection_kwargs["password"] = params.get("password", None)
            pool.reset()

        return pool


class ClusterConnectionFactory(ConnectionFactory):
    """A connection factory compatible with redis.cluster.RedisCluster

    The cluster client manages connection pools internally, so we don't want to
    do it at this level like the base ConnectionFactory does.
    """

    # A global cache of URL->client so that within a process, we will reuse a
    # single client, and therefore a single set of connection pools.
    _clients = {}
    _clients_lock = threading.Lock()

    def __init__(self, options):
        # set appropriate default, but allow overriding client class
        options.setdefault("REDIS_CLIENT_CLASS", "redis.cluster.RedisCluster")
        super().__init__(options)

    def connect(self, url: str) -> RedisCluster:
        """Given a connection url, return a client instance.

        Prefer to return from our cache but if we don't yet have one build it
        to populate the cache.
        """
        if url not in self._clients:
            with self._clients_lock:
                if url not in self._clients:
                    self._clients[url] = self._connect(url)
        return self._clients[url]

    def _connect(self, url: str) -> RedisCluster:
        """
        Given a connection url, return a new client instance.

        Basic django-redis ConnectionFactory manages a cache of connection
        pools and builds a fresh client each time. because the cluster client
        manages its own connection pools, we will instead merge the
        "connection" and "client" kwargs and throw them all at the client to
        sort out.

        If we find conflicting client and connection kwargs, we'll raise an
        error.
        """
        # Get connection and client kwargs...
        connection_params = self.make_connection_params(url)
        client_cls_kwargs = deepcopy(self.redis_client_cls_kwargs)

        # ... and smash 'em together (crashing if there's conflicts)...
        for key, value in connection_params.items():
            if key in client_cls_kwargs:
                raise ImproperlyConfigured(
                    f"Found '{key}' in both the connection and the client kwargs"
                )
            client_cls_kwargs[key] = value

        # ... and then build and return the client
        return self.redis_client_cls(**client_cls_kwargs)

    def disconnect(self, connection: RedisCluster):
        connection.disconnect_connection_pools()


class SentinelConnectionFactory(ConnectionFactory):
    def __init__(self, options):
        # allow overriding the default SentinelConnectionPool class
        options.setdefault(
            "CONNECTION_POOL_CLASS", "redis.sentinel.SentinelConnectionPool"
        )
        super().__init__(options)

        sentinels = options.get("SENTINELS")
        if not sentinels:
            error_message = "SENTINELS must be provided as a list of (host, port)."
            raise ImproperlyConfigured(error_message)

        # provide the connection pool kwargs to the sentinel in case it
        # needs to use the socket options for the sentinels themselves
        connection_kwargs = self.make_connection_params(None)
        connection_kwargs.pop("url")
        connection_kwargs.update(self.pool_cls_kwargs)
        self._sentinel = Sentinel(
            sentinels,
            sentinel_kwargs=options.get("SENTINEL_KWARGS"),
            **connection_kwargs,
        )

    def get_connection_pool(self, params):
        """
        Given a connection parameters, return a new sentinel connection pool
        for them.
        """
        url = urlparse(params["url"])

        # explicitly set service_name and sentinel_manager for the
        # SentinelConnectionPool constructor since will be called by from_url
        cp_params = dict(params)
        cp_params.update(service_name=url.hostname, sentinel_manager=self._sentinel)
        pool = super().get_connection_pool(cp_params)

        # convert "is_master" to a boolean if set on the URL, otherwise if not
        # provided it defaults to True.
        is_master = parse_qs(url.query).get("is_master")
        if is_master:
            pool.is_master = to_bool(is_master[0])

        return pool


def get_connection_factory(path=None, options=None):
    if path is None:
        path = getattr(
            settings,
            "DJANGO_REDIS_CONNECTION_FACTORY",
            "django_redis.pool.ConnectionFactory",
        )
    opt_conn_factory = options.get("CONNECTION_FACTORY")
    if opt_conn_factory:
        path = opt_conn_factory

    cls = import_string(path)
    return cls(options or {})
