import sys
import inspect

from pprint import PrettyPrinter
import sys

PPRINT = PrettyPrinter(indent=4, width=sys.maxsize).pprint

import json
d=json.dumps({'a':{'a1':1}, 'b':'zzzz zzzz zzzz zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'})

PPRINT(d)



def test1():
	def test2():
		print('x')
	test2()

test1()
