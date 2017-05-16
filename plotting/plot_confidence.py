#!/usr/bin/env python

import matplotlib.pyplot as plt

append = "50bootstrap"
append500 = "500bootstrap"

def read_dat(name):
  iters_, values_, lower_, upper_ = [], [], [], []
  with open("../data/students_%s.dat" % name) as f:
    for line in f.readlines():
      (x, value, low, up) = tuple(line.split(" "))
      iters_ += [int(x)]
      values_ += [float(value)]
      lower_ += [float(low)]
      upper_ += [float(up)]
  return (iters_, values_, lower_, upper_)

(iters, values, lower, upper) = read_dat(append)
(iters500, values500, lower500, upper500) = read_dat(append500)

true_value = values[-1]

# Plot raw values
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(iters, values, "-x", label="Approximate Answer")
ax.plot(iters, lower, "-x", label="Lower Confidence Bound")
ax.plot(iters, upper, "-x", label="Upper Confidence Bound")
ax.plot(iters, [true_value]*len(iters), "--", label="True Answer")
ax.set_xlabel("Iteration")
ax.set_ylabel("Answer")
ax.legend(loc = "upper right")
plt.savefig("output_" + append + ".png")

# Plot interval size
interval_size = [upper[i] - lower[i] for i in range(len(lower))]
interval_size500 = [upper500[i] - lower500[i] for i in range(len(lower500))]

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(iters, interval_size, "-", label="50 bootstrap trials", color="gray")
ax.plot(iters500, interval_size500, "-", label="500 bootstrap trials", color="red")
ax.set_xlabel("Iteration")
ax.set_ylabel("Confidence Interval Size")
ax.legend()
plt.savefig("output2_" + append + ".png")

# Plot error (Note: this is not correct!?)
true_value = values[-1]
error = [100 * (true_value - v) / true_value for v in values]
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(iters, error, "-x", label="error")
ax.plot(iters, [0]*len(error), "--", label="error")
ax.set_xlabel("Iteration")
#ax.set_ylim([-0.9, 0.1])
ax.set_ylabel("True Error (%)")
plt.savefig("output3_" + append + ".png")

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
plt.savefig("output4_" + append + ".png")


