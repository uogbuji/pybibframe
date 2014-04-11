#From amara.saxtools (Amara 1.x)
#See e.g. http://code.activestate.com/recipes/265881-a-sax-filter-for-normalizing-text-events/ but not main code is outdated

from xml.sax.saxutils import XMLFilterBase

class normalize_text_filter(XMLFilterBase):
    """
    SAX filter to ensure that contiguous white space nodes are
    delivered merged into a single node
    """
    def __init__(self, *args):
        XMLFilterBase.__init__(self, *args)
        self._accumulator = []
        return

    def _complete_text_node(self):
        if self._accumulator:
            XMLFilterBase.characters(self, ''.join(self._accumulator))
            self._accumulator = []
        return

    def startDocument(self):
        XMLFilterBase.startDocument(self)
        return

    def endDocument(self):
        XMLFilterBase.endDocument(self)
        return

    def startElement(self, name, attrs):
        self._complete_text_node()
        XMLFilterBase.startElement(self, name, attrs)
        return

    def startElementNS(self, name, qname, attrs):
        self._complete_text_node()
        #A bug in Python 2.3 means that we can't just defer to parent, which is broken
        #XMLFilterBase.startElementNS(self, name, qname, attrs)
        self._cont_handler.startElementNS(name, qname, attrs)
        return

    def endElement(self, name):
        self._complete_text_node()
        XMLFilterBase.endElement(self, name)
        return

    def endElementNS(self, name, qname):
        self._complete_text_node()
        XMLFilterBase.endElementNS(self, name, qname)
        return

    def processingInstruction(self, target, body):
        self._complete_text_node()
        XMLFilterBase.processingInstruction(self, target, body)
        return

    def characters(self, text):
        self._accumulator.append(text)
        return

    def ignorableWhitespace(self, ws):
        self._accumulator.append(text)
        return

    def parse(self, source):
        # Delegate to XMLFilterBase for the rest
        XMLFilterBase.parse(self, source)
        return

