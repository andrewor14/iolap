#!/usr/bin/env python

import matplotlib.pyplot as plt
from os import makedirs
from os.path import exists
import sys

def main():
  name = "slaq_10pools_500bootstrap_students"
  args = sys.argv
  if len(args) == 2:
    name = args[1]
  (iters, values, lower, upper) = read_dat(name)

  # Make plotting dir if it doesn't already exist
  if not exists(name):
    makedirs(name)

  # Plot raw values
  true_slaq_value = values[-1]
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(iters, values, "-x", label="Approximate Answer")
  ax.plot(iters, lower, "-x", label="Lower Confidence Bound")
  ax.plot(iters, upper, "-x", label="Upper Confidence Bound")
  ax.plot(iters, [true_slaq_value] * len(iters), "--", label="True Answer")
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Answer")
  ax.legend()
  ax.set_title(name)
  plt.savefig("%s/answers.png" % name)
  # Plot interval size
  interval_size = [upper[i] - lower[i] for i in range(len(lower))]
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(iters, interval_size, "-", label=name, color="red")
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Confidence Interval Size")
  ax.legend()
  ax.set_title(name)
  plt.savefig("%s/loss.png" % name)
 
def read_dat(name):
  '''
  Parse computed values and bounds from the specified data file.
  '''
  iters_, values_, lower_, upper_ = [], [], [], []
  with open("../data/%s/pool1.dat" % name) as f:
    for i, line in enumerate(f.readlines()):
      (_, value, low, up) = tuple(line.split(" "))
      iters_ += [i + 1]
      values_ += [float(value)]
      lower_ += [float(low)]
      upper_ += [float(up)]
  return (iters_, values_, lower_, upper_)

if __name__ == "__main__":
  main()

