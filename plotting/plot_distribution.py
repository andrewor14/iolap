#!/usr/bin/env python

import matplotlib.pyplot as plt
import math
import numpy as np
import scipy.stats as stats
import sys

if len(sys.argv) <= 1:
  print "Usage: plot_distribution.py [file_name]"
  sys.exit(1)

h = []
with open(sys.argv[1]) as f:
  h = [float(x) for x in f.readlines()]

#h.sort()
hmean = np.mean(h)
hstd = np.std(h)
pdf = stats.norm.pdf(h, hmean, hstd)
#plt.plot(h, pdf) # including h here is crucial
plt.text(0, len(h), "mean = %s, var = %s" % (hmean, math.pow(hstd, 2)), fontsize=15)
plt.plot(h)
plt.plot([hmean] * len(h))
plt.show()

