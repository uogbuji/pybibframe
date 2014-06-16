# pybibframe

<!-- Once on PyPI can simplify to: pip install pybibframe -->

Requires Python 3.4 or more recent. To install dependencies:

    pip install -r requirements.txt

Then install pybibframe:

    python setup.py install

# Usage

## Converting MARC/XML to RDF or Versa output

Note: Versa is a model for Web resources and relationships. Think of it as an evolution of Resource Description Framework (RDF) that's at once simpler and more expressive. It's the default internal representation for PyBibframe, though regular RDF is an optional output.

    marc2bf records.mrx

Reads MARC/XML from the file records.mrx and outputs a Versa representation of the resulting BIBFRAME records in JSON format. You can send that output to a file as well:

    marc2bf -o resources.versa.json records.mrx

If you want an RDF/Turtle representation of this file you can do:

    marc2bf -o resources.versa.json -r resources.ttl records.mrx

You can process more than one MARC/XML file at a time by listing them on the command line:

    marc2bf records1.mrx records2.mrx records3.mrx

Or by using wildcards:

    marc2bf records?.mrx

PyBibframe is highly extensible, and you can specify plug-ins from the command line. You need to specify the Python module from which the plugins can be imported and a configuration file specifying how the plugins are to be used. For example, to use the `linkreport` plugin that comes with PyBibframe you can do:

    marc2bfrdf -c config1.json --mod=bibframe.plugin records.mrx

Where the contents of config1.json might be:

	{
	    "plugins": [
	        {"id": "https://github.com/uogbuji/pybibframe#linkreport",
	        "output-file": "linkreport.html"}
	    ]
	}

Which in this case will generate, in addition to the regular outputs will create a file named `linkreport.html` listing any resource fields in the form of URIs.


# See also

Some open-source tools for working with BIBFRAME (see http://bibframe.org)


Note: very useful to have around yaz-marcdump

Download from http://ftp.indexdata.com/pub/yaz/ , unpack then do:

$ ./configure --prefix=$HOME/.local
$ make && make install

