import subprocess

def run_command(n_workers):
    command = f"./run_mpi.sh {n_workers} ~/bin/vector_mul_tfgmpi"
    #command = f"./run_mpi.sh {n_workers} ~/bin/vec_mul_mpi"
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print("Test n_workers:", n_workers, "Result: ", result.stdout.strip())
    
    return float(result.stdout.strip())  # Assuming the output is a single number

def plot_results():
    #n_workers_list = [2, 4, 8, 16]  # Adjust the list of worker counts as needed
    n_workers_list = [1, 2, 4, 8, 16, 32, 64, 128, 208]  # Adjust the list of worker counts as needed
    results = []

    for n_workers in n_workers_list:
        result = run_command(n_workers)
        results.append(result)

    print(results)

if __name__ == "__main__":
    plot_results()
