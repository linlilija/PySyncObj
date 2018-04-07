from __future__ import print_function
import sys
import pickle
from functools import wraps
from subprocess import Popen, PIPE
import os
import matplotlib.pyplot as plt
DEVNULL = open(os.devnull, 'wb')

START_PORT = 4321
MIN_RPS = 10
MAX_RPS = 40000

def memoize(fileName):
    def doMemoize(func):
        if os.path.exists(fileName):
            with open(fileName) as f:
                cache = pickle.load(f)
        else:
            cache = {}
        @wraps(func)
        def wrap(*args):
            if args not in cache:
                cache[args] = func(*args)
                with open(fileName, 'wb') as f:
                    pickle.dump(cache, f)
            return cache[args]
        return wrap
    return doMemoize

def singleBenchmark(requestsPerSecond, requestSize, numNodes, numNodesReadonly = 0, delay = False):
    rpsPerNode = requestsPerSecond / (numNodes + numNodesReadonly)
    cmd = [sys.executable, 'testobj_delay.py' if delay else 'testobj.py', str(rpsPerNode), str(requestSize)]
    #cmd = 'python2.7 -m cProfile -s time testobj.py %d %d' % (rpsPerNode, requestSize)
    processes = []
    allAddrs = []
    for i in range(numNodes):
        allAddrs.append('localhost:%d' % (START_PORT + i))
    for i in range(numNodes):
        addrs = list(allAddrs)
        selfAddr = addrs.pop(i)
        p = Popen(cmd + [selfAddr] + addrs, stdin=PIPE)
        processes.append(p)
    for i in range(numNodesReadonly):
        p = Popen(cmd + ['readonly'] + allAddrs, stdin=PIPE)
        processes.append(p)
    errRates = []
    for p in processes:
        p.communicate()
        errRates.append(float(p.returncode) / 100.0)
    avgRate = sum(errRates) / len(errRates)
    print('average success rate:', avgRate)
    if delay:
        return avgRate
    return avgRate >= 0.9

def doDetectMaxRps(requestSize, numNodes):
    a = MIN_RPS
    b = MAX_RPS
    numIt = 0
    while b - a > MIN_RPS:
        c = a + (b - a) / 2
        res = singleBenchmark(c, requestSize, numNodes)
        if res:
            a = c
        else:
            b = c
        print('subiteration %d, current max %d' % (numIt, a))
        numIt += 1
    return a

# @memoize('maxRpsCache.bin')
def detectMaxRps(requestSize, numNodes):
    results = []
    for i in range(0, 1):
        res = doDetectMaxRps(requestSize, numNodes)
        print('iteration %d, current max %d' % (i, res))
        results.append(res)
    return sorted(results)[len(results) // 2]

def printUsage():
    print('Usage: %s mode(delay/rps/custom)' % sys.argv[0])
    sys.exit(-1)


def measure_RPS_vs_Clustersize():
    """Measure max RPS as a function of cluster size."""
    cluster_size = [i for i in range(3, 8)]
    rps = []
    for i in cluster_size:
        res = detectMaxRps(200, i)
        print('nodes number: %d, rps: %d' % (i, int(res)))
        rps.append(res)
    plt.plot(cluster_size, rps)
    plt.xlabel("Cluster Size")
    plt.ylabel("RPS")
    plt.title("RPS vs Cluster Size")
    plt.show()


def measure_RPS_vs_Requestsize():
    """Measure max RPS as a function of request size."""
    request_size = [i for i in range(10, 2100, 500)]
    rps = []
    for i in request_size:
        res = detectMaxRps(i, 3)
        print('request size: %d, rps: %d' % (i, int(res)))
        rps.append(res)
    plt.plot(request_size, rps)
    plt.xlabel("Request Size")
    plt.ylabel("RPS")
    plt.title("RPS vs Request Size")
    plt.show()


if __name__ == '__main__':

    if len(sys.argv) != 2:
        printUsage()

    mode = sys.argv[1]
    if mode == 'delay':
        print('Average delay:', singleBenchmark(50, 10, 5, delay=True))
    elif mode == 'rps':
        measure_RPS_vs_Clustersize()
        measure_RPS_vs_Requestsize()

    elif mode == 'custom':
        singleBenchmark(25000, 10, 3)
    else:
        printUsage()
