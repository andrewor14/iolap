#!/usr/bin/env python

import matplotlib.pyplot as plt
from bisect import bisect_left
from os import listdir, makedirs
from os.path import exists, join

fair_name = "fair_10pools_500bootstrap"
slaq_name = "slaq_10pools_500bootstrap"

def read_files(paths):
  '''
  Parse loss data over time from the specified files.

  Time here is expressed in terms of number of seconds elapsed since the beginning of
  the experiment. Loss is expressed in terms of confidence interval size at a given time.
  '''
  times, losses = [], []
  for path in paths:
    time_arr, loss_arr = [], []
    with open(path) as f:
      for line in f.readlines():
        (x, value, low, up) = tuple(line.split(" "))
        time_arr += [int(x)]
        loss_arr += [float(up) - float(low)]
    times += [time_arr]
    losses += [loss_arr]
  min_time = min(min(time_arr) for time_arr in times)
  times = [[(time - min_time) / 1000.0 for time in time_arr] for time_arr in times]
  return (times, losses)

def average_loss(times, losses):
  '''
  Compute average losses over time.
  '''
  # Flatten, dedup, and sort
  all_times = sorted(list(set([t for time_arr in times for t in time_arr])))
  all_avg_losses = []
  for time in all_times:
    total_loss = 0
    loss_count = 0
    avg_loss = 0
    for (i, loss_arr) in enumerate(losses):
      nearest_index = bisect_left(times[i], time)
      if nearest_index != len(times[i]) and (nearest_index > 0 or time == times[i][nearest_index]):
        loss_count += 1
        total_loss += loss_arr[nearest_index]
    if loss_count != 0:
      avg_loss = total_loss / loss_count
    all_avg_losses += [avg_loss]
  return (all_times, all_avg_losses)

def main():
  slaq_dir = "../data/%s" % slaq_name
  fair_dir = "../data/%s" % fair_name
  slaq_files = [join(slaq_dir, f) for f in listdir(slaq_dir) if f.endswith(".dat")]
  fair_files = [join(fair_dir, f) for f in listdir(fair_dir) if f.endswith(".dat")]
  (slaq_times, slaq_losses) = read_files(slaq_files)
  (fair_times, fair_losses) = read_files(fair_files)

  # Make output dir if it doesn't already exist
  if not exists(slaq_name):
    makedirs(slaq_name)

  # Plot raw values
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  for i in range(len(slaq_times)):
    ax.plot(slaq_times[i], slaq_losses[i], "-x", label="Query %s" % i)
  ax.set_xlabel("Time (s)")
  ax.set_ylabel("Loss")
  ax.legend(loc = "upper right")
  plt.savefig("%s/concurrent_loss.png" % slaq_name)

  # Plot deltas
  delta_times = [time_arr[:-1] for time_arr in slaq_times]
  delta_losses = [[(loss_arr[i+1] - loss_arr[i]) for i in range(len(loss_arr)-1)]
    for loss_arr in slaq_losses]
  delta_losses = [[loss_arr[i] / min(loss_arr[:i+1]) for i in range(len(loss_arr))]
    for loss_arr in delta_losses]
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  for i in range(len(slaq_times)):
    ax.plot(delta_times[i][:-1], delta_losses[i][:-1], "-", label="Query %s" % i)
  ax.set_xlabel("Time (s)")
  ax.set_ylabel("Normalized delta loss")
  ax.legend(loc = "upper right")
  plt.savefig("%s/concurrent_delta_loss.png" % slaq_name)

  # Plot average overall loss
  (avg_slaq_times, avg_slaq_losses) = average_loss(slaq_times, slaq_losses)
  (avg_fair_times, avg_fair_losses) = average_loss(fair_times, fair_losses)
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(avg_slaq_times, avg_slaq_losses, "-", label="SLAQ average loss")
  ax.plot(avg_fair_times, avg_fair_losses, "-", label="Fair average loss")
  ax.set_xlabel("Time (s)")
  ax.set_ylabel("Loss")
  ax.legend(loc = "upper right")
  plt.savefig("%s/concurrent_avg_loss.png" % slaq_name)

if __name__ == '__main__':
  main()

