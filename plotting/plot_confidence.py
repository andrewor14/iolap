#!/usr/bin/env python

import matplotlib.pyplot as plt

iters = []
values = []
lower = []
upper = []

with open("students_100.dat") as f:
  for line in f.readlines():
    (x, value, low, up) = tuple(line.split(" "))
    iters += [int(x)]
    values += [float(value)]
    lower += [float(low)]
    upper += [float(up)]

# Plot raw values
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(iters, values, "-x", label="values")
ax.plot(iters, lower, "-x", label="lower")
ax.plot(iters, upper, "-x", label="upper")
ax.set_xlabel("Iteration")
ax.set_ylabel("Answer")
ax.legend(loc = "lower right")
plt.savefig("output.png")

# Plot error (Note: this is not correct!)
true_value = values[-1]
error = [100 * (true_value - v) / true_value for v in values]
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(iters, error, "-x", label="error")
ax.set_xlabel("Iteration")
ax.set_ylabel("Error (%)")
plt.savefig("output2.png")

