'''
marc2bfrdf -v -o /tmp/ph1.ttl -s /tmp/ph1.stats.js -b http://example.org test/resource/princeton-holdings1.xml 2> /tmp/ph1.log

'''

import re
import os
import sys
import time
import logging
import string
import itertools

from versa import I, ORIGIN, RELATIONSHIP, TARGET
from versa.util import simple_lookup
from amara.lib.util import coroutine
from amara.lib import iri

from bibframe import BFZ, BFLC, g_services

LABEL_REL = I(iri.absolutize('label', BFZ))

FIELD008_STATSINFO = ('field008', '021109s1999    ch acf        000 0 chi d')
LEADER_STATSINFO = ('leader', '02045nam a2200000 a 4500')

def packedchar_stats(statsinfo, val, stats):
    '''
    Update stats for field 008, which is a packed character flag array.
    Stats are a list with a dict for each 008 character position, mapping characters to count of their appearances at that position
    '''
    key, sample = statsinfo
    if not key in stats:
        stats[key] = []
        for i in range(len(sample)):
            stats[key].append({})
    for i, c in enumerate(val):
        stats[key][i].setdefault(c, 0)
        stats[key][i][c] += 1
    return


@coroutine
def statshandler(sid=None, model=None, stats=None, **kwargs):
    if stats is None: stats = {}
    while True:
        params = yield
        stats.setdefault('recordcount', 0)
        stats['recordcount'] += 1

        for stmt in itertools.chain(model.match(subj=params['workid']), *[ model.match(subj=iid) for iid in params['instanceids'] ] ):
            pred = stmt[RELATIONSHIP]
            fulltag = None
            if pred.startswith(BFZ):
                pred = pred.partition(BFZ)[-1]
            if pred.startswith(u'tag-'):
                pred = pred.partition(u'tag-')[-1]
                fulltag = pred
            stats.setdefault('rels', {}).setdefault(pred, 0)
            stats['rels'][pred] += 1
            if fulltag:
                if len(fulltag) == 4:
                    fulltag = fulltag[:-1]
                    print (2, fulltag)
                    stats['rels'].setdefault(fulltag, 0)
                    stats['rels'][fulltag] += 1

        #for sf in params[u'subfields']:
        #    codes_stats = stats.setdefault('codes', {}).setdefault(code+u'$'+sf, 0)
        #    stats['codes'][code+u'$'+sf] += 1

        creators = [ simple_lookup(model, c[TARGET], LABEL_REL) for c in model.match(params['workid'], I(iri.absolutize('creator', BFZ))) ]

        stats.setdefault('details', [])
        stmts = list(model.match(params['workid'], I(iri.absolutize('title', BFZ))))
        title = stmts[0][TARGET] if stmts else ''
        for k, setv in params[u'special_properties'].iteritems():
            for v in setv:
                stats['details'].append([params[u'workid'], title, creators, k, v])
                stats.setdefault('characteristics', {})
                if v not in stats['characteristics']: stats['characteristics'][v] = 0
                stats['characteristics'][v] += 1

        packedchar_stats(FIELD008_STATSINFO, params[u'field008'], stats)
        packedchar_stats(LEADER_STATSINFO, params[u'leader'], stats)

statshandler.iri = u'https://github.com/uogbuji/pybibframe#stats'
