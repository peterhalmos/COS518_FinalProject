import subprocess
import os

latencies = {}
for num_nodes in [2**i for i in range(3, 13)]:
    try:
        result = subprocess.run(["python", os.path.dirname(os.path.abspath(__file__)) + "/chord_latency_test.py", str(num_nodes)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout_str = result.stdout.decode('utf-8').split("\n")[-1]
        print(stdout_str)
        latencies[num_nodes] = float(stdout_str)
        print("Completed simulation for", num_nodes, "nodes! Latency:", latencies[num_nodes])
    except subprocess.CalledProcessError as e:
        print("Error:", e)

print(latencies)