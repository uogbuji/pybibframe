# bibframe

from versa import I

BFZ = I('http://bibfra.me/vocab/')
BFLC = I('http://bibframe.org/vocab/')

#A way to register services to specialize bibframe.py processing
#Maps URL to callable
g_services = {}

def register_service(coro, iri=None):
    iri = iri or coro.iri
    g_services[iri] = coro
    return

