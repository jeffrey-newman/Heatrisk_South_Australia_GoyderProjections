import rpy2.robjects.tests
import unittest

# the verbosity level can be increased if needed
tr = unittest.TextTestRunner(verbosity = 1)
suite = rpy2.robjects.tests.suite()
tr.run(suite)