import subprocess
import os

chord_latencies = {}
num_hops = {}
koorde_latencies = {}
for num_nodes in [2**i for i in range(3, 11)]:
    try:
        result = subprocess.run(["python", os.path.dirname(os.path.abspath(__file__)) + "/chord_latency_test.py", str(num_nodes)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout_str = result.stdout.decode('utf-8').split("\n")[-1]
        print(stdout_str)
        chord_latencies[num_nodes], num_hops[num_nodes] = [float(i) for i in stdout_str.split()]
        print("Completed chord simulation for", num_nodes, "nodes! Latency:", chord_latencies[num_nodes], "Num hops:", num_hops[num_nodes])
    except subprocess.CalledProcessError as e:
        print("Error:", e)
    # try:
    #     result = subprocess.run(["python", os.path.dirname(os.path.abspath(__file__)) + "/koorde_latency_test.py", str(num_nodes)], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     stdout_str = result.stdout.decode('utf-8').split("\n")[-1]
    #     print(stdout_str)
    #     koorde_latencies[num_nodes] = float(stdout_str)
    #     print("Completed koorde simulation for", num_nodes, "nodes! Latency:", koorde_latencies[num_nodes])
    # except subprocess.CalledProcessError as e:
    #     print("Error:", e)

print("Chord latencies:", chord_latencies, "Num hops:", num_hops)