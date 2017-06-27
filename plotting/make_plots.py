#!/usr/bin/env python

from os import listdir
from os.path import exists
import plot_confidence
import plot_concurrent

data_dir = "../data"
files = [f for f in listdir(data_dir) if f.startswith("slaq") and not exists(f)]

for f in files:
  print "Plotting experiment '%s'" % f
  try:
    plot_confidence.plot(f)
    plot_concurrent.plot(f)
  except Exception as e:
    print '  ERROR: %s' % e

