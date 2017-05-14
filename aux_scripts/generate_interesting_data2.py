import random, json, string
import csv
from numpy import random as nrand
import sys
GB = 0.01
count = int(1.1163*(2**22) * GB)
fields = ["name", "index", "coin_toss", "normal", "zipf", "uniform", 
          "twin_peak", "geometric", "exponential", "lognormal", "zero"]
with open("interesting_data.csv", "wb") as fp:
    writer = csv.writer(fp)
    writer.writerow(fields)
    for i  in range(count):
        if i % int(count/20) == 0:
            print "{0:.0f}%... ".format(float(i)/count * 100),
            sys.stdout.flush()
        name = random.choice(string.uppercase)
        for j in range(random.randint(1,7)):
            name += random.choice("aeiou") + random.choice(string.lowercase)
        entry = [name, 
                 i,
                 nrand.randint(0, 2),
                 nrand.normal(10000, 1000),
                 nrand.zipf(2.0),
                 int(nrand.uniform(10000)),
                 nrand.choice([nrand.normal(1000, 200), nrand.normal(4000,200)]),
                 nrand.geometric(0.35),
                 int(nrand.exponential(1.0)*100),
                 nrand.lognormal(3.0, 1.0),
                 0
                ]
        writer.writerow(entry)
print "Done."
