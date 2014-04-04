'''
'''

import re
import os
import sys
import time
import logging
import string
import itertools

from datachef.ids import simple_hashstring

import amara
from amara.lib.util import coroutine
from amara.thirdparty import httplib2, json
from amara.lib import U
from amara.lib.util import element_subtree_iter
from amara.lib import iri
from amara import namespaces

from bibframe.isbnplus import isbn_list
from bibframe.reader.marcpatterns import MATERIALIZE, MATERIALIZE_VIA_ANNOTATION, FIELD_RENAMINGS, INSTANCE_FIELDS
from bibframe.reader.marcextra import process_leader, process_008

from versa import I


#canonicalize_isbns
#from btframework.augment import lucky_viaf_template, lucky_idlc_template, DEFAULT_AUGMENTATIONS

FALINKFIELD = u'856u'
#CATLINKFIELD = u'010a'
CATLINKFIELD = u'LCCN'
CACHEDIR = os.path.expanduser('~/tmp')

BFV = I('http://bibframe.org/vocab/')

NON_ISBN_CHARS = re.compile(u'\D')

def invert_dict(d):
    #http://code.activestate.com/recipes/252143-invert-a-dictionary-one-liner/#c3
    #See also: http://pypi.python.org/pypi/bidict
        #Though note: http://code.activestate.com/recipes/576968/#c2
    inv = {}
    for k, v in d.iteritems():
        keys = inv.setdefault(v, [])
        keys.append(k)
    return inv

#One of the records gives us:

#http://hdl.loc.gov/loc.mss/eadmss.ms009216

#Which links for METS download to:

#http://hdl.loc.gov/loc.mss/eadmss.ms009216.4

#Which redirects to:

#http://findingaids.loc.gov/mastermets/mss/2009/ms009216.xml

PREFIXES = {u'ma': 'http://www.loc.gov/MARC21/slim', u'me': 'http://www.loc.gov/METS/'}

RDFTYPE = I(namespaces.RDF_NAMESPACE + 'type')

def idgen(idbase):
    '''
    Generate a IRI
    '''
    #Simple tumbler for now, possibly switch to random number, with some sort of sequence override for unit testing
    while True:
        ix = 0
        yield iri.absolutize(str(ix), idbase) if idbase else str(ix)
        ix += 1


def hashidgen(idbase, key):
    '''
    Generate an IRI as a hash of given information
    '''
    h = simple_hashstring(key)
    return iri.absolutize(h, idbase) if idbase else str(h)


#iri.absolutize('label', PROPBASE),
#{"@context": "http://copia.ogbuji.net#_metadata"}


def instancegen(isbns):
    '''
    Default handling of the idea of splitting a MARC record with FRBR Work info as well as instances signalled by ISBNs
    '''
    base_instance_id = instance_item[u'id']
    instance_ids = []
    subscript = ord(u'a')
    for subix, (inum, itype) in enumerate(isbn_list(isbns)):
        #print >> sys.stderr, subix, inum, itype
        subitem = instance_item.copy()
        subitem[u'isbn'] = inum
        subitem[u'id'] = base_instance_id + (unichr(subscript + subix) if subix else u'')
        if itype: subitem[u'isbnType'] = itype
        instance_ids.append(subitem[u'id'])
        new_instances.append(subitem)


#FIXME: Stuff to be made thread local
T_prior_materializedids = set()

RESOURCE_TYPE = 'marcrType'

def process(recs, relsink, idbase, logger=logging):
    '''
    
    '''
    idg = idgen(idbase)
    #FIXME: Use thread local storage rather than function attributes

    #A few code modularization functions pulled into local context as closures
    def process_materialization(lookup, subfields, code=None):
        materializedid = hashidgen(idbase, tuple(subfields.items()))
        #The extra_props are parameters inherent to a particular MARC field/subfield for purposes of linked data representation
        if code is None: code = lookup
        (subst, extra_props) = MATERIALIZE[lookup]
        if RESOURCE_TYPE in extra_props:
            relsink.add(I(materializedid), RDFTYPE, I(iri.absolutize(extra_props[RESOURCE_TYPE], BFV)))
        logger.debug((lookup, subfields, extra_props))

        if materializedid in T_prior_materializedids:
            #Just bundle in the subfields as they are, to avoid throwing out data. They can be otherwise used or just stripped later on
            for k, v in itertools.chain((u'marccode', code), subfields.iteritems(), extra_props.iteritems()):
                if k == RESOURCE_TYPE: continue
                relsink.add(I(materializedid), iri.absolutize(k, BFV), v)

        return materializedid, subst


    def process_annotation(anntype, subfields, extra_annotation_props):
        #Separate annotation subfields from object subfields
        object_subfields = subfields.copy()
        annotation_subfields = {}
        for k, v in subfields.items():
            if code+k in ANNOTATIONS_FIELDS:
                annotation_subfields[k] = v
                del object_subfields[k]

        #objectid = idg.next()
        #object_props.update(object_subfields)

        annotationid = idg.next()
        relsink.add(I(annotationid), RDFTYPE, I(iri.absolutize(anntype, BFV)))
        for k, v in itertools.chain(annotation_subfields.items(), extra_annotation_props.items()):
            relsink.add(I(annotationid), I(iri.absolutize(k, BFV)), v)

        #Return enough info to generate the main subject/object relationship. The annotation is taken care of at this point
        return annotationid, object_subfields


    for rec in recs:
        leader = U(rec.xml_select(u'ma:leader', prefixes=PREFIXES))
        #Add work item record
        workid = idg.next()
        relsink.add(I(workid), RDFTYPE, I(iri.absolutize('Work', BFV)))
        instanceid = idg.next()
        relsink.add(I(instanceid), RDFTYPE, I(iri.absolutize('Instance', BFV)))
        #relsink.add((instanceid, iri.absolutize('leader', PROPBASE), leader))
        relsink.add(I(workid), I(iri.absolutize('hasInstance', BFV)), I(instanceid))

        for cf in rec.xml_select(u'ma:controlfield', prefixes=PREFIXES):
            code = U(cf.xml_select(u'@tag'))
            key = u'cftag_' + code
            val = U(cf)
            if code == '008':
                field008 = val
            if list(cf.xml_select(u'ma:subfield', prefixes=PREFIXES)):
                for sf in cf.xml_select(u'ma:subfield', prefixes=PREFIXES):
                    sfcode = U(sf.xml_select(u'@code'))
                    sfval = U(sf)
                    #For now assume all leader fields are instance level
                    relsink.add(I(instanceid), I(iri.absolutize('MARC' + key + sfcode, BFV)), sfval)
            else:
                #For now assume all leader fields are instance level
                relsink.add(I(instanceid), I(iri.absolutize('MARC' + key, BFV)), val)

        for df in rec.xml_select(u'ma:datafield', prefixes=PREFIXES):
            code = U(df.xml_select(u'@tag'))
            key = u'dftag_' + code
            val = U(df)
            handled = False
            if list(df.xml_select(u'ma:subfield', prefixes=PREFIXES)):
                subfields = dict(( (U(sf.xml_select(u'@code')), U(sf)) for sf in df.xml_select(u'ma:subfield', prefixes=PREFIXES) ))
                lookup = code
                #See if any of the field codes represents a reference to an object which can be materialized

                if code in MATERIALIZE:
                    materializedid, subst = process_materialization(code, subfields)
                    subject = instanceid if code in INSTANCE_FIELDS else workid
                    relsink.add(I(subject), I(iri.absolutize(subst, BFV)), I(materializedid))
                    print >> sys.stderr, '.',
                    handled = True

                if code in MATERIALIZE_VIA_ANNOTATION:
                    #FIXME: code comments for extra_object_props & extra_annotation_props
                    (subst, anntype) = MATERIALIZE_VIA_ANNOTATION[code]
                    annotationid, object_subfields = process_annotation(anntype, subfields, extra_annotation_props)

                    subject = instanceid if code in INSTANCE_FIELDS else workid
                    objectid = idg.next()
                    relsink.add(I(subject), I(iri.absolutize(subst, BFV)), I(objectid), {I(iri.absolutize('annotation', BFV)): I(annotationid)})

                    for k, v in itertools.chain((u'marccode', code), object_subfields.items(), extra_object_props.items()):
                        relsink.add(I(objectid), I(iri.absolutize(k, BFV)), v)

                    print >> sys.stderr, '.',
                    handled = True

                #See if any of the field+subfield codes represents a reference to an object which can be materialized
                if not handled:
                    for k, v in subfields.items():
                        lookup = code + k
                        if lookup in MATERIALIZE:
                            #XXX At first glance you'd think you can always derive code from lookup (e.g. lookup[:3] but what if e.g. someone trims the left zero fill on the codes in the serialization?
                            materializedid, subst = process_materialization(lookup, subfields, code=code)
                            subject = instanceid if code in INSTANCE_FIELDS else workid
                            relsink.add(I(subject), I(iri.absolutize(subst, BFV)), I(materializedid))

                            #Is the MARC code part of the hash computation for the materiaalized object ID? Surely not!
                            #materializedid = hashidgen((code,) + tuple(subfields.items()))
                            print >> sys.stderr, '.',
                            handled = True

                        else:
                            field_name = u'dftag/' + lookup
                            if lookup in FIELD_RENAMINGS:
                                field_name = FIELD_RENAMINGS[lookup]
                            #Handle the simple field_name substitution of a label name for a MARC code
                            subject = instanceid if code in INSTANCE_FIELDS else workid
                            relsink.add(I(subject), I(iri.absolutize(field_name, BFV)), v)

            #print >> sys.stderr, lookup, key
            subject = instanceid if code in INSTANCE_FIELDS else workid
            relsink.add(I(subject), I(iri.absolutize(u'dftag/' + code, BFV)), val)


        special_properties = {}
        for k, v in process_leader(leader):
            special_properties.setdefault(k, set()).add(v)

        for k, v in process_008(field008):
            special_properties.setdefault(k, set()).add(v)

        #We get some repeated values out of leader & 008 processing, and we want to
        #Remove dupes so we did so by working with sets then converting to lists
        for k, v in special_properties.items():
            special_properties[k] = list(v)
            for item in v:
            #logger.debug(v)
                relsink.add(I(instanceid), I(iri.absolutize(k, BFV)), item)


        #reduce lists of just one item
        #for k, v in work_item.items():
        #    if type(v) is list and len(v) == 1:
        #        work_item[k] = v[0]
        #work_sink.send(work_item)

        
        #Handle ISBNs re: https://foundry.zepheira.com/issues/1976
        isbn_stmts = relsink.match(subj=instanceid, pred=iri.absolutize(u'isbn', BFV))
        isbns = [ s[2] for s in isbn_stmts ]
        other_instance_ids = []
        subscript = ord(u'a')
        newid = None
        for subix, (inum, itype) in enumerate(isbn_list(isbns)):
            #print >> sys.stderr, subix, inum, itype
            newid = idg.next()
            duplicate_statements(relsink, instanceid, newid)
            relsink.add(I(newid), I(iri.absolutize(u'isbn', BFV)), inum)
            #subitem[u'id'] = instanceid + (unichr(subscript + subix) if subix else u'')
            if itype: relsink.add(I(newid), I(iri.absolutize(u'isbnType', BFV)), I(itype))
            other_instance_ids.append(newid)

        if not other_instance_ids:
            #Make sure it's created as an instance even if it has no ISBN
            relsink.add(I(workid), I(iri.absolutize(u'instance', BFV)), I(instanceid))

        for iid in other_instance_ids:
            relsink.add(I(workid), I(iri.absolutize(u'instance', BFV)), I(iid))

        #if newid is None: #No ISBN specified
        #    send_instance(ninst)

        #ix += 1
        print >> sys.stderr, '+',

    return

def duplicate_statements(model, oldsubject, newsubject):
    for stmt in model.match(oldsubject):
        s, p, o, c = stmt
        model.add(I(newsubject), p, o, c)
        return

