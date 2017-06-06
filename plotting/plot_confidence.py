#!/usr/bin/env python

import matplotlib.pyplot as plt

name = "slaq_10pools_500bootstrap"

def main():
  (iters, values, lower, upper) = read_dat(name)
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
  plt.savefig("%s.png" % name)
  # Plot interval size
  interval_size = [upper[i] - lower[i] for i in range(len(lower))]
  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  ax.plot(iters, interval_size, "-", label=name, color="red")
  ax.set_xlabel("Iteration")
  ax.set_ylabel("Confidence Interval Size")
  ax.legend()
  plt.savefig("%s_loss.png" % name)
 
def read_dat(name):
  '''
  Parse computed values and bounds from the specified data file.
  '''
  iters_, values_, lower_, upper_ = [], [], [], []
  with open("../data/%s/pool1.dat" % name) as f:
    for line in f.readlines():
      (x, value, low, up) = tuple(line.split(" "))
      iters_ += [int(x)]
      values_ += [float(value)]
      lower_ += [float(low)]
      upper_ += [float(up)]
  return (iters_, values_, lower_, upper_)

if __name__ == "__main__":
  main()

