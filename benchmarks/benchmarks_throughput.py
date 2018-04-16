from __future__ import print_function
import sys
import pickle
from functools import wraps
from subprocess import Popen, PIPE
import os
DEVNULL = open(os.devnull, 'wb')

START_PORT = 4321


def singleBenchmark(numNodes, quorumSize1=0, quorumSize2=0, drop_ratio=0.0, numNodesReadonly=0, delay=False):
    """Execute benchmark."""
    cmd = [sys.executable, 'testobj_throughput.py', str(quorumSize1), str(quorumSize2), str(drop_ratio)]
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
    num_success = []
    for p in processes:
        p.communicate()
        num_success.append(p.returncode)
    print(num_success)
    avgRPS = sum(num_success) / 20.0
    print('average RPS:', avgRPS)
    return avgRPS


def test_flexible_raft(drop_ratio):
    """Measure RPS vs cluster size of flexible Raft"""
    cluster_size = [i for i in range(3, 10, 2)]
    for i in cluster_size:
        rps = []
        for j in range(0, min(i//2+1, 4)):
            res = singleBenchmark(i, i + 1 - j, j, drop_ratio) if j != 0 else singleBenchmark(i, 0, 0, drop_ratio)
            rps.append(res)
        filename = "result_%d_%f" % (i, drop_ratio)
        with open(filename, 'a') as f:
            f.write("RPS with cluster size = %d & drop ratio = %f" % (i, drop_ratio))
            f.write(str(rps))


if __name__ == '__main__':

    # set quorum size for phase 2: 0 -> normal Raft, >0 -> flexible Raft
    quorumSize2 = 0

    # set message loss rate
    drop_ratio = [-1, 0.01, 0.05]

    for r in drop_ratio:
        test_flexible_raft(r)
