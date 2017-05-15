#!/usr/bin/env python

import matplotlib.pyplot as plt

iters = []
values = []
lower = []
upper = []

append = "500bootstrap"

with open("/Users/robertmacdavid/Documents/Github/andrewor/iolap/data/students_500bootstrap.dat") as f:
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
plt.savefig("output" + append + ".png")

# Plot interval size
true_value = values[-1]
interval_size = [upper[i] - lower[i] for i in range(len(lower))]

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(iters, interval_size, "-x", label="Interval Size")
ax.set_xlabel("Iteration")
ax.set_ylabel("Confidence Interval Size")
plt.savefig("output2" + append + ".png")

# Plot error (Note: this is not correct!?)
true_value = values[-1]
error = [100 * (true_value - v) / true_value for v in values]
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(iters, error, "-x", label="error")
ax.set_xlabel("Iteration")
ax.set_ylabel("True Error (%)")
plt.savefig("output3" + append + ".png")

# Plot delta error versus delta confidence interval size
delta_iters = iters[:-2]
delta_error = [-abs(error[i+1]) + abs(error[i]) for i in range(len(delta_iters))]


max_delta_error = max([abs(delta) for delta in delta_error])
delta_error = [error/max_delta_error for error in delta_error]

delta_interval_size = [-abs(interval_size[i+1]) + abs(interval_size[i]) for i in range(len(delta_iters))]
max_delta_interval = max([abs(delta) for delta in delta_interval_size])
delta_interval_size = [size/max_delta_interval for size in delta_interval_size]
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(delta_iters, delta_interval_size, "-x", label="Normalized Delta Interval Size")
ax.plot(delta_iters, delta_error, "-x", label="Normalized Delta Error")
ax.set_xlabel("Iteration")
ax.set_ylabel("Normalized Delta")
ax.legend(loc = "lower right")
plt.savefig("output4" + append + ".png")


