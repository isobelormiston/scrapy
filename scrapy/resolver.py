from twisted.internet import defer
from twisted.internet.base import ThreadedResolver
from twisted.internet.interfaces import IHostnameResolver, IResolutionReceiver, IResolverSimple
from zope.interface.declarations import implementer, provider

from scrapy.utils.datatypes import LocalCache


# TODO: cache misses

dnscache = LocalCache(10000)

class CachingThreadedResolver(ThreadedResolver):
    def __init__(self, reactor, cache_size, timeout):
        print("XXX CachingThreadedResolver initialising")
        super(CachingThreadedResolver, self).__init__(reactor)
        dnscache.limit = cache_size
        self.timeout = timeout

    def getHostByName(self, name, timeout=None):
        print("XXX CachingThreadedResolver getHostByName")
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
        print("XXX CachingThreadedResolver _cache_result")
        dnscache[name] = result
        return result


@implementer(IHostnameResolver)
class CachingHostnameResolver:
    """
    Experimental caching resolver. Resolves IPv4 and IPv6 addresses,
    does not support setting a timeout value for DNS requests.
    """

    def __init__(self, reactor, cache_size):
        print("XXX CachingHostnameResolver (new) initialising")
        self.reactor = reactor
        self.original_resolver = reactor.nameResolver
        dnscache.limit = cache_size

    @classmethod
    def from_crawler(cls, crawler, reactor):
        print("XXX CachingHostnameResolver (new) from_crawler")
        if crawler.settings.getbool('DNSCACHE_ENABLED'):
            cache_size = crawler.settings.getint('DNSCACHE_SIZE')
        else:
            cache_size = 0
        return cls(reactor, cache_size)

    def install_on_reactor(self):
        print("XXX CachingHostnameResolver (new) install_on_reactor")
        self.reactor.installNameResolver(self)

    def resolveHostName(self, resolutionReceiver, hostName, portNumber=0,
                        addressTypes=None, transportSemantics='TCP'):
        print("XXX CachingHostnameResolver (new) resolveHostName")

        @provider(IResolutionReceiver)
        class CachingResolutionReceiver(resolutionReceiver):

            def __init__(self):
                super().__init__()
                self.resolved_ipv4_addresses = []

            def resolutionBegan(self, resolution):
                print("XXX CachingHostnameResolver (new) resolutionBegan")
                super(CachingResolutionReceiver, self).resolutionBegan(resolution)
                self.resolution = resolution
                self.resolved = False

            def addressResolved(self, address):
                print("XXX CachingHostnameResolver (new) addressResolved")
                import twisted
                if type(address) != twisted.internet.address.IPv6Address:
                    print("XXX ipv4 address")
                    self.resolved_ipv4_addresses.append(address)
                    return
                print("XXX ipv6 address")
                super(CachingResolutionReceiver, self).addressResolved(address)
                self.resolved = True

            def resolutionComplete(self):
                print("XXX CachingHostnameResolver (new) resolutionComplete")
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
            print("XXX CachingHostnameResolver (new) KeyError")
            return self.original_resolver.resolveHostName(
                CachingResolutionReceiver(),
                hostName,
                portNumber,
                addressTypes,
                transportSemantics
            )