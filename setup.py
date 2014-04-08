from distutils.core import setup

setup(
    name = 'pybibframe',
    version = '0.3',
    description = 'Python tools for BIBFRAME',
    license = 'License :: OSI Approved :: Apache Software License',
    author = 'Uche Ogbuji',
    author_email = 'uche@zepheira.com',
    url = 'http://zepheira.com/',
    package_dir={'bibframe': 'lib'},
    packages = ['bibframe', 'bibframe.reader', 'bibframe.writer'],
    scripts=['exec/marc2bfrdf', 'exec/marcbin2xml'],
    #http://packages.python.org/distribute/setuptools.html#declaring-dependencies
#    install_requires = ['amara >= 2', 'uritemplate >= 0.5.1'],
)
