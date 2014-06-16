'''
cat config.js

{
    "plugins": [
        {"id": "https://github.com/uogbuji/pybibframe#linkreport",
        "output-file": "/tmp/linkreport.html"}
    ]
}

marc2bfrdf -c test/resource/config1.json --mod=bibframe.plugin test/resource/2003542664.xml

'''

import os
import json
import itertools

from versa import I, ORIGIN, RELATIONSHIP, TARGET
from versa.util import simple_lookup

from amara3.util import coroutine
from amara3 import iri

from bibframe import BFZ, BFLC, g_services

ISBN_REL = I(iri.absolutize('isbn', BFZ))
TITLE_REL = I(iri.absolutize('title', BFZ))

BFHOST = 'bibfra.me'

#The plugin is a coroutine of the main MARC conversion routine
#It's pushed (through yield) adetails from each MARC/XML record processed

#Parameters:
#params['model']: raw Versa model with converted resource information from the MARC
#params['workid']: ID of the work constructed from the MARC record
#params['instanceid']: list of IDs of instances constructed from the MARC record
@coroutine
def linkreport(config=None, **kwargs):
    #Any configuration variables passed in
    if config is None: config = {}
    try:
        #Initialize the output
        outstr = ''
        while True:
            params = yield
            model = params['model']
            items = {}
            #Get the title
            #First get the work ID
            workid = params['workid']
            #simple_lookup() is a little helper for getting a property from a resource
            title = simple_lookup(model, workid, TITLE_REL)
            #Get the ISBN, just pick the first one
            isbn = ''
            if params['instanceids']:
                inst1 = params['instanceids'][0]
                isbn = simple_lookup(model, inst1, ISBN_REL)

            envelope = '<div id="{0} isbn="{1}"><title>{2}</title>\n'.format(workid, isbn, title)
            #iterate over all the relationship targets to see which is a link
            for stmt in model.match():
                if iri.matches_uri_syntax(stmt[TARGET]) and iri.split_uri_ref(stmt[TARGET])[1] != BFHOST:
                    envelope += '<a href="{0}">{0}</a>\n'.format(stmt[TARGET], stmt[TARGET])
            envelope += '</div>\n'
            outstr += envelope
    except GeneratorExit:
        #Reached when close() is called on this coroutine
        with open(config['output-file'], "w") as outf:
            outf.write(outstr)

linkreport.iri = 'https://github.com/uogbuji/pybibframe#linkreport'
