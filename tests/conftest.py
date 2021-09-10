import sys
import os

# Note: This line forces the test suite to import the dmci package in the current source tree
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
