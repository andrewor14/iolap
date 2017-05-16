#!/usr/bin/env python

import matplotlib.pyplot as plt
from bisect import bisect_left

path = "/Users/robertmacdavid/Documents/Github/andrewor/iolap/data/"


def read_files(filenames):
    times = []
    losses = []
    for filename in filenames:
        time_arr = []
        loss_arr = []
        with open(path + filename) as f:
            for line in f.readlines():
                (x, value, low, up) = tuple(line.split(" "))
                time_arr += [int(x)]
                loss_arr += [float(up) - float(low)]
        times += [time_arr]
        losses += [loss_arr]

    min_time = min(min(time_arr) for time_arr in times)
    times = [[(time - min_time)/1000.0 for time in time_arr] for time_arr in times]

    return (times, losses)



def average_loss(losses, times):
    all_times = set([])
    for time_arr in times:
        all_times.update(time_arr)
    all_times = sorted(list(all_times))

    all_avg_losses = []
    for time in all_times:
        total_loss = 0
        loss_count = 0
        for (i, loss_arr) in enumerate(losses):
            nearest_index = bisect_left(times[i], time)
            if nearest_index != len(times[i]) and (nearest_index > 0 or time == times[i][nearest_index]):
                loss_count += 1
                total_loss += loss_arr[nearest_index]

        avg_loss = 0
        if loss_count != 0:
            avg_loss = total_loss/loss_count
        all_avg_losses += [avg_loss]
    return (all_times, all_avg_losses)



def main():

    filenames = ["students_concurrent2_naga" + str(i) + ".dat" for i in range(1, 6)]
    (slaq_times, slaq_losses) = read_files(filenames)

    filenames2 = ["students_concurrent2_naga" + str(i) + "_fair.dat" for i in range(1, 6)]
    (fair_times, fair_losses) = read_files(filenames2)

    # Plot raw values
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    for i in range(len(slaq_times)):
        ax.plot(slaq_times[i], slaq_losses[i], "-x", label="Query " + str(i))

    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Loss")
    ax.legend(loc = "upper right")
    plt.savefig("output-concurrent.png")

    # Plot deltas
    delta_times = [time_arr[:-1] for time_arr in slaq_times]
    delta_losses = [[(loss_arr[i+1] - loss_arr[i]) for i in range(len(loss_arr)-1)]
                    for loss_arr in slaq_losses]


    delta_losses = [[loss_arr[i]/min(loss_arr[:i+1]) for i in range(len(loss_arr))]
                    for loss_arr in delta_losses]


    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    for i in range(len(slaq_times)):
        ax.plot(delta_times[i][:-1], delta_losses[i][:-1], "-", label="Query " + str(i))

    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Normalized Delta Loss")
    ax.legend(loc = "upper right")
    plt.savefig("output-concurrent-delta-loss.png")



    # plot average overall loss
    (avg_slaq_times, avg_slaq_losses) = average_loss(slaq_losses, slaq_times)
    (avg_fair_times, avg_fair_losses) = average_loss(fair_losses, fair_times)

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(avg_slaq_times, avg_slaq_losses, "-", label="SLAQ Average System Loss")
    ax.plot(avg_fair_times, avg_fair_losses, "-", label="Fair Average System Loss")

    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Loss")
    ax.legend(loc = "upper right")
    plt.savefig("output-concurrent-avg-loss.png")



if __name__ == '__main__':
    main()








