#!/usr/bin/env python3

import sys
import zipfile

_, outfile, sources = sys.argv[0], sys.argv[1], sys.argv[2:]

module_dirs = {
	'grakn/service',
	'grakn/service/Session',
	'grakn/service/Session/util',
	'grakn/service/Session/autogenerated',
	'grakn/service/Keyspace',
	'grakn/service/Keyspace/autogenerated',
	'grakn/service/Session/Concept',
	'grakn/exception',
}

with zipfile.ZipFile(outfile, 'w') as outzip:
    for sourcefn in sources:
        arcfn = sourcefn[sourcefn.find('grakn/'):]
        outzip.write(sourcefn, arcfn)
    for module in module_dirs:
    	outzip.writestr(module + "/__init__.py", '')