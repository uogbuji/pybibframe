'''
For processing MARC/XML
Uses SAX for streaming process
Notice however, possible security vulnerabilities:
'''

from xml import sax
#from xml.sax.handler import ContentHandler
#from xml.sax.saxutils import XMLGenerator

#from bibframe.contrib.xmlutil import normalize_text_filter
from bibframe.reader.marc import LEADER, CONTROLFIELD, DATAFIELD


MARCXML_NS = "http://www.loc.gov/MARC21/slim"

#Subclass from ContentHandler in order to gain default behaviors
class marcxmlhandler(sax.ContentHandler):
    def __init__(self, sink, *args, **kwargs):
        self._sink = sink
        self._getcontent = False
        sax.ContentHandler.__init__(self, *args, **kwargs)

    def startElementNS(self, name, qname, attributes):
        (ns, local) = name
        if ns == MARCXML_NS:
            #if local == u'collection':
            #    return
            if local == u'record':
                self._record = []
            elif local == u'leader':
                self._chardata_dest = [LEADER, u'']
                self._record.append(self._chardata_dest)
                self._getcontent = True
            elif local == u'controlfield':
                self._chardata_dest = [CONTROLFIELD, attributes[None, u'tag'], u'']
                self._record.append(self._chardata_dest)
                self._getcontent = True
            elif local == u'datafield':
                self._record.append([DATAFIELD, attributes[None, u'tag'], attributes.copy(), []])
            elif local == u'subfield':
                self._chardata_dest = [attributes[None, u'code'], u'']
                self._record[-1][3].append(self._chardata_dest)
                self._getcontent = True
        return

    def characters(self, data):
        if self._getcontent:
            self._chardata_dest[-1] += data

    def endElementNS(self, name, qname):
        (ns, local) = name
        if ns == MARCXML_NS:
            if local == u'record':
                self._sink.send(self._record)
            elif local in (u'leader', u'controlfield', u'subfield'):
                self._getcontent = False
        return


def parse_marcxml(inp, sink):
    '''
    Parse a MARC/XML document and yield each record's data
    '''
    parser = sax.make_parser()
    #parser.setContentHandler(marcxmlhandler(receive_recs()))
    parser.setFeature(sax.handler.feature_namespaces, 1)

    #downstream_handler = marcxmlhandler(receive_recs())
    #upstream, the parser, downstream, the next handler in the chain
    #parser.setContentHandler(downstream_handler)
    #normparser = normalize_text_filter(parser)
    #normparser.parse(inp)

    handler = marcxmlhandler(sink)
    #upstream, the parser, downstream, the next handler in the chain
    parser.setContentHandler(handler)
    parser.parse(inp)

