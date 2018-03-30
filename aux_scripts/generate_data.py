#!/usr/bin/env python

import random, json, string
import csv
from numpy import random as nrand
import os
import sys

GB = 30
count = int(1.1163*(2**22) * GB)
fields = ["name", "index", "coin_toss", "normal", "zipf", "uniform", 
          "twin_peak", "geometric", "exponential", "lognormal", "zero"]
path = "/disk/local/disk2/stafman/students30g_3.json"
abs_path = os.path.abspath(path)
with open(abs_path, "w") as fp:
    #writer = csv.writer(fp)
    #writer.writerow(fields)
    print "Writing to %s..." % abs_path
    for i  in range(count):
        if i % int(count/20) == 0:
            print "{0:.0f}%... ".format(float(i)/count * 100),
            sys.stdout.flush()
        name = random.choice(string.uppercase)
        for j in range(random.randint(1,7)):
            name += random.choice("aeiou") + random.choice(string.lowercase)
        entry = {"name":        name, 
#                 "index":       i,
#                 "coin_toss":   nrand.randint(0, 2),
#                 "ct":  nrand.randint(0, 2),
                 "normal":      nrand.normal(10000, 1000),
#                 "zipf":        nrand.zipf(2.0),
                 "uniform":     int(nrand.uniform(-5000,5000)),
#                 "twin_peak":   nrand.choice([nrand.normal(1000, 200), nrand.normal(4000,200)]),
#                 "geometric":   nrand.geometric(0.35),
#                 "exponential": int(nrand.exponential(1.0)*100),
#                 "lognormal":   nrand.lognormal(3.0, 1.0),
#                 "zero":        0,
                 "fivegroup": i % 5
                }
        json.dump(entry, fp)
        fp.write("\n")
print "Done."

