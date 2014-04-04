import logging

from testconfig import config

from versa.driver import memory

#If you do this you also need --nologcapture
#Handle  --tc=debug:y option
if config.get('debug', 'n').startswith('y'):
    logging.basicConfig(level=logging.DEBUG)

PREFIXES = {u'ma': 'http://www.loc.gov/MARC21/slim', u'me': 'http://www.loc.gov/METS/'}

#Move to a test utils module
import os, inspect
def module_path(local_function):
   ''' returns the module path without the use of __file__.  Requires a function defined 
   locally in the module.
   from http://stackoverflow.com/questions/729583/getting-file-path-of-imported-module'''
   return os.path.abspath(inspect.getsourcefile(local_function))

#hack to locate test resource (data) files regardless of from where nose was run
RESOURCEPATH = os.path.normpath(os.path.join(module_path(lambda _: None), '../../resource/'))

def test_basic_marc1():
    import os
    import amara
    from bibframe.reader.marc import process
    indoc = amara.parse(os.path.join(RESOURCEPATH, 'kford-holdings1.xml'))
    #Top level ma:collection is optional, so can't just assume /ma:collection/ma:record XPath
    recs = indoc.xml_select(u'//ma:record', prefixes=PREFIXES)
    #logging.debug(recs)
    m = memory.connection()
    m.create_space()
    process(recs, m, idbase='http://example.org/')
    logging.debug('MARC BASICS PART 1')
    #for stmt in m:
    #    logging.debug('Result: {0}'.format(repr(stmt)))
        #assert result == ()

