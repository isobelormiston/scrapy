from twisted.internet import defer
from twisted.internet.base import ThreadedResolver
from twisted.internet.interfaces import IHostnameResolver, IResolutionReceiver, IResolverSimple
from zope.interface.declarations import implementer, provider

from scrapy.utils.datatypes import LocalCache


# TODO: cache misses

dnscache = LocalCache(10000)

class CachingThreadedResolver(ThreadedResolver):
    def __init__(self, reactor, cache_size, timeout):
        super(CachingThreadedResolver, self).__init__(reactor)
        dnscache.limit = cache_size
        self.timeout = timeout

    def getHostByName(self, name, timeout=None):
        if name in dnscache:
            return defer.succeed(dnscache[name])
        # in Twisted<=16.6, getHostByName() is always called with
        # a default timeout of 60s (actually passed as (1, 3, 11, 45) tuple),
        # so the input argument above is simply overridden
        # to enforce Scrapy's DNS_TIMEOUT setting's value
        timeout = (self.timeout,)
        d = super(CachingThreadedResolver, self).getHostByName(name, timeout)
        if dnscache.limit:
            d.addCallback(self._cache_result, name)
        return d

    def _cache_result(self, result, name):
        dnscache[name] = result
        return result


@implementer(IHostnameResolver)
class CachingHostnameResolver:
    """
    Experimental caching resolver. Resolves IPv4 and IPv6 addresses,
    does not support setting a timeout value for DNS requests.
    """

    def __init__(self, reactor, cache_size):
        self.reactor = reactor
        self.original_resolver = reactor.nameResolver
        dnscache.limit = cache_size

    @classmethod
    def from_crawler(cls, crawler, reactor):
        if crawler.settings.getbool('DNSCACHE_ENABLED'):
            cache_size = crawler.settings.getint('DNSCACHE_SIZE')
        else:
            cache_size = 0
        return cls(reactor, cache_size)

    def install_on_reactor(self):
        self.reactor.installNameResolver(self)

    def resolveHostName(self, resolutionReceiver, hostName, portNumber=0,
                        addressTypes=None, transportSemantics='TCP'):

        @provider(IResolutionReceiver)
        class CachingResolutionReceiver(resolutionReceiver):

            def __init__(self):
                super().__init__()
                self.resolved_ipv4_addresses = []

            def resolutionBegan(self, resolution):
                super(CachingResolutionReceiver, self).resolutionBegan(resolution)
                self.resolution = resolution
                self.resolved = False

            def addressResolved(self, address):
                import twisted
                if type(address) != twisted.internet.address.IPv6Address:
                    print("XXX ipv4 address")
                    self.resolved_ipv4_addresses.append(address)
                    return
                print("XXX ipv6 address")
                super(CachingResolutionReceiver, self).addressResolved(address)
                self.resolved = True

            def resolutionComplete(self):
                #super(CachingResolutionReceiver, self).resolutionComplete()
                if self.resolved:
                    dnscache[hostName] = self.resolution
                elif self.resolved_ipv4_addresses:
                    ipv4 = self.resolved_ipv4_addresses[0]
                    super(CachingResolutionReceiver, self).addressResolved(ipv4)
                    dnscache[hostName] = self.resolution
                super(CachingResolutionReceiver, self).resolutionComplete()

        try:
            print("End of resolveHostname")
            print("XXX HOSTNAME {}".format(dnscache[hostName]))
            return dnscache[hostName]
        except KeyError:
            return self.original_resolver.resolveHostName(
                CachingResolutionReceiver(),
                hostName,
                portNumber,
                addressTypes,
                transportSemantics
            )