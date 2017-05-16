#!/usr/bin/env python

import matplotlib.pyplot as plt
from bisect import bisect_left

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

file1 = "students_concurrent_naga1.dat"
file2 = "students_concurrent_naga2.dat"
path = "/Users/robertmacdavid/Documents/Github/andrewor/iolap/data/"


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
ax.plot(times1, size1, "-x", label="iOLAP Query 1")
ax.plot(times2, size2, "-x", label="iOLAP Query 2")

ax.set_xlabel("Time (s)")
ax.set_ylabel("Loss")
ax.legend(loc = "upper right")
plt.savefig("output-concurrent.png")



filenames = ["students_concurrent2_naga" + str(i) + ".dat" for i in range(1, 6)]
slaq_times = []
slaq_losses = []
for filename in filenames:
    time_arr = []
    loss_arr = []
    with open(path + filename) as f:
        for line in f.readlines():
            (x, value, low, up) = tuple(line.split(" "))
            time_arr += [int(x)]
            loss_arr += [float(up) - float(low)]
    slaq_times += [time_arr]
    slaq_losses += [loss_arr]



def average_loss(losses, times):
    all_times = set([])
    for time_arr in times:
        all_times.update(time_arr)
    all_times = sorted(list(all_times))

    results = {}
    for time in all_times:
        total_loss = 0
        loss_count = 0
        for (i, loss_arr) in enumerate(losses):
            nearest_index = bisect_left(times[i], time)
            if nearest_index != len(times[i]) and nearest_index > 0:
                loss_count += 1
                total_loss += loss_arr[nearest_index]

        avg_loss = 0
        if loss_count != 0:
            avg_loss = total_loss/loss_count
        results[time] = avg_loss
    return results

filenames = ["students_concurrent2_naga" + str(i) + ".dat" for i in range(1, 6)]
slaq_times = []
slaq_losses = []
for filename in filenames:
    time_arr = []
    loss_arr = []
    with open(path + filename) as f:
        for line in f.readlines():
            (x, value, low, up) = tuple(line.split(" "))
            time_arr += [int(x)]
            loss_arr += [float(up) - float(low)]
    slaq_times += [time_arr]
    slaq_losses += [loss_arr]

loss_dict = average_loss(slaq_losses, slaq_times)

slaq_times = sorted(loss_dict.keys())
slaq_losses = [loss_dict[time] for time in slaq_times]

# Plot raw values
fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
ax.plot(slaq_times, slaq_losses, "-x", label="Total System Loss")

ax.set_xlabel("Time (s)")
ax.set_ylabel("Loss")
ax.legend(loc = "upper right")
plt.savefig("output-concurrent-avg-loss.png")








