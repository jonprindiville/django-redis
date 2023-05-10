from ..pool import ClusterConnectionFactory
from .default import DefaultClient


class ClusterClient(DefaultClient):
    """A django-redis client compatible with redis.cluster.RedisCluster

    We don't do much different here, except for using our own
    ClusterConnectionFactory (the base class would instead use the value of the
    DJANGO_REDIS_CONNECTION_FACTORY setting, but we don't care about that
    setting here.)
    """

    def __init__(self, server, params, backend) -> None:
        super().__init__(server, params, backend)
        self.connection_factory = ClusterConnectionFactory(options=self._options)
