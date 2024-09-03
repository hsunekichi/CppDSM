import matplotlib.pyplot as plt


results_cluster = [3022, 1704, 1650, 1220, 1107, 1003, 896, 790, 970]
results_cluster_mpi = [1644, 1637, 1634, 1600, 1603, 1592, 1615, 1621, 2117]

results_tfg = [10193.0, 6472.0, 4088.0, 3148.0, 2899.0, 3104.0]
results_mpi = [8909.0, 5464.0, 3491.0, 2303.0, 1837.0, 1645.0]

n_workers_cluster = [1, 3, 4, 5, 6, 8, 12, 16, 24]
n_workers_list = [8, 16, 32, 64, 128, 208]  # Adjust the list of worker counts as needed
#n_workers_list = [2, 4, 8, 16]

#tfg on blue, mpi on red
plt.plot(n_workers_cluster, results_cluster, marker="o", color='b')
plt.plot(n_workers_cluster, results_cluster_mpi, marker="o", color='r')
plt.legend(["TFG", "MPI"])
plt.xlabel("Number of workers")
plt.ylabel("Execution time (s)")
plt.title("Execution time vs. number of workers")
plt.grid()
plt.show()