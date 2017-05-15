#!/usr/bin/env python

import matplotlib.pyplot as plt

iters1 = []
values1 = []
lower1 = []
upper1 = []
size1 = []

iters2 = []
values2 = []
lower2 = []
upper2 = []
size2 = []

append = "500bootstrap"
file1 = "students_concurrent_naga1.dat"
file2 = "students_concurrent_naga2.dat"

with open("/Users/robertmacdavid/Documents/Github/andrewor/iolap/data/" + file1) as f1:
  for line in f1.readlines():
    (x, value, low, up) = tuple(line.split(" "))
    iters1 += [int(x)]
    values1 += [float(value)]
    lower1 += [float(low)]
    upper1 += [float(up)]
    size1 += [float(up) - float(low)]

with open("/Users/robertmacdavid/Documents/Github/andrewor/iolap/data/" + file2) as f2:
  for line in f2.readlines():
    (x, value, low, up) = tuple(line.split(" "))
    iters2 += [int(x)]
    values2 += [float(value)]
    lower2 += [float(low)]
    upper2 += [float(up)]
    size2 += [float(up) - float(low)]


minTime = min(min(iters1), min(iters2))
times1 = [(time - minTime)/1000.0 for time in iters1]
times2 = [(time - minTime)/1000.0 for time in iters2]


# Plot raw values
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(times1, size1, "-x", label="Loss1")
ax.plot(times2, size2, "-x", label="Loss2")

ax.set_xlabel("Time (s)")
ax.set_ylabel("Loss")
ax.legend(loc = "upper right")
plt.savefig("output-concurrent.png")


