from __future__ import print_function
import sys
import pickle
from functools import wraps
from subprocess import Popen, PIPE
import os
DEVNULL = open(os.devnull, 'wb')

START_PORT = 4321

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

def singleBenchmark(requestsPerSecond, requestSize, numNodes, quorumSize1=0, quorumSize2=0,
                    drop_ratio=0.0, numNodesReadonly=0, delay=False):
    """Execute benchmark."""
    rpsPerNode = requestsPerSecond / (numNodes + numNodesReadonly)
    cmd = [sys.executable, 'testobj_delay.py' if delay else 'testobj.py', str(rpsPerNode), str(requestSize),
           str(quorumSize1), str(quorumSize2), str(drop_ratio)]
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
        errRates.append(float(p.returncode) / 10000.0)
    avgRate = sum(errRates) / len(errRates)
    if delay:
        return avgRate
    return avgRate >= 0.9

def test_flexible_raft_latency():
    """Measure Latency vs cluster size of flexible Raft"""
    cluster_size = [i for i in range(3, 10, 2)]
    fixedRps = 50
    for i in cluster_size:
        for j in range(0, min(i // 2 + 1, 5)):
            res = singleBenchmark(fixedRps, 10, i, i + 1 - j, j, delay=True) if j != 0 else singleBenchmark(fixedRps, 10, i, 0, 0, delay=True)
            print('cluster size {}, q1 {}, q2 {}'.format(i, 0 if j == 0 else i - j + 1, j))
            print('Average delay:', res)

if __name__ == '__main__':
    test_flexible_raft_latency()