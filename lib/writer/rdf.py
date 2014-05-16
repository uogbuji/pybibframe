'''
'''

import re
import os
import sys
import logging
import itertools

#from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib import URIRef, Literal

#from datachef.ids import simple_hashstring

from amara.lib import iri
from amara import namespaces

from versa import I, SUBJECT, RELATIONSHIP, VALUE

from bibframe import BFZ, BFLC

RDFTYPE = namespaces.RDF_NAMESPACE + 'type'

WORKCLASS = iri.absolutize('Work', BFZ)
INSTANCECLASS = iri.absolutize('Instance', BFZ)
INSTANCEREL = iri.absolutize('hasInstance', BFZ)

def prep(stmt):
    '''
    Prepare a statement into a triple ready for rdflib
    '''
    s, p, o = stmt[:3]
    s = URIRef(s)
    p = URIRef(p)
    o = URIRef(o) if isinstance(o, I) else Literal(o)
    return s, p, o


def process(source, target, logger=logging):
    '''
    Take an in-memory BIBFRAME model and convert it into an rdflib graph

    '''
    #Hoover up everything with a type
    for stmt in source.match(None, RDFTYPE, None):
        rid = stmt[SUBJECT]
        [ target.add(prep(stmt)) for stmt in source.match(rid) ]

    return
