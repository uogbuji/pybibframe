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

BFV = 'http://bibframe.org/vocab/'
RDFTYPE = namespaces.RDF_NAMESPACE + 'type'

WORKCLASS = iri.absolutize('Work', BFV)
INSTANCECLASS = iri.absolutize('Instance', BFV)
INSTANCEREL = iri.absolutize('hasInstance', BFV)

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
    #Start with the works
    #XXX: What if there are orphan instances?
    for stmt in source.match(None, RDFTYPE, WORKCLASS):
        workid = stmt[SUBJECT]
        [ target.add(prep(wstmt)) for wstmt in source.match(workid) ]
        #[ target.add(*wstmt[:3]) for wstmt in source.match(workid) ]
        for wstmt in source.match(workid, INSTANCEREL):
            instanceid = stmt[SUBJECT]
            [ target.add(prep(wstmt)) for wstmt in source.match(instanceid) ]
            #target.add(*wstmt[:3])

    return
