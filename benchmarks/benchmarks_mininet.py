from __future__ import print_function
import sys
# sys.path.append("../")
import pickle
from functools import wraps
from subprocess import Popen, PIPE
import os
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from sys import argv
from time import sleep
import random
import math
import time
import json


DEVNULL = open(os.devnull, 'wb')

START_PORT = 8000
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


def singleBenchmark(requestsPerSecond, requestSize, numNodes, quorumSize1, quorumSize2, host_list, delay=False):
    """Execute benchmark."""
    # Distribute requests to nodes evenly 
    rpsPerNode = requestsPerSecond // numNodes
    # cmd = [sys.executable, 'testobj.py', str(rpsPerNode), str(requestSize), str(quorumSize1), str(quorumSize2), str(0)]
    cmd = [sys.executable, 'testobj_delay.py' if delay else 'testobj.py', str(rpsPerNode), str(requestSize),
           str(quorumSize1), str(quorumSize2), str(drop_ratio)]
    processes = []
    allAddrs = []
    for i in range(numNodes):
        allAddrs.append('%s:%d' % (host_list[i][0], START_PORT + i))
    for i in range(numNodes):
        addrs = list(allAddrs)
        selfAddr = addrs.pop(i)
        c = " ".join(cmd+[selfAddr]+addrs)
        p = host_list[i][1].popen(c, stdin=PIPE)
        processes.append(p)
    errRates = []
    for p in processes:
        p.communicate()
        errRates.append(float(p.returncode) / 100.0)
    avgRate = sum(errRates) / len(errRates)
    # print('average success rate:', avgRate)
    if delay:
        return avgRate
    return avgRate >= 0.9

def vote(c, requestSize, numNodes, quorumSize1, quorumSize2, host_list):
    t1 = t2 = 0
    for i in range(3):
        ret = singleBenchmark(c, requestSize, numNodes, quorumSize1, quorumSize2, host_list)
        if ret:
            t1 += 1
        else:
            t2 += 1
    return t1 > t2


def doDetectMaxRps(requestSize, numNodes, quorumSize1, quorumSize2, host_list):
    """Measure max RPS with binary search"""
    a = MIN_RPS
    b = MAX_RPS
    numIt = 0
    while b - a > MIN_RPS:
        c = a + (b - a) / 2
        res = vote(c, requestSize, numNodes, quorumSize1, quorumSize2, host_list)
        if res:
            a = c
        else:
            b = c
        print('subiteration %d, current max %d' % (numIt, a))
        numIt += 1
    return a


# @memoize('maxRpsCache.bin')
def detectMaxRps(requestSize, numNodes, quorumSize1, quorumSize2, host_list):
    """Measure max RPS three times and use median as result."""
    results = []
    for i in range(0, 1):
        res = doDetectMaxRps(requestSize, numNodes, quorumSize1, quorumSize2, host_list)
        print('iteration %d, current max %d' % (i, res))
        results.append(res)
    return sorted(results)[len(results) // 2]


class SingleSwitchTopo(Topo):
    """Single switch connected to n hosts."""
    def __init__(self, n=2, drop_ratio=0, delayMin=0.0, delayAvg=0.0, delayStddev=0.0, lossy=False, **opts):
        Topo.__init__(self, **opts)
        self.__delayMin = math.ceil(delayMin)
        self.__delayAvg = delayAvg
        self.__delayStddev = delayStddev
        switch = self.addSwitch('s1')

        for h in range(n):
            # randDelay = str(self.__getRandomDelay()) + 'ms'
            host = self.addHost('h%s' % (h + 1), cpu=.9 / n)
            self.addLink(host, switch, bw=100, delay='5ms', loss=drop_ratio, use_htb=True)

    def __getRandomDelay(self):
        delay = math.ceil(random.gauss(self.__delayAvg, self.__delayStddev))
        if delay < self.__delayMin:
            return self.__delayMin
        return delay


def test_flexible_raft(drop_ratio):
    """Measure RPS vs cluster size of flexible Raft"""
    cluster_size = [i for i in range(3, 8, 2)]
    # test different phase 2 quorum size
    fixedRps = 2000
    for i in cluster_size:
        """Create network"""
        # topo = SingleSwitchTopo(i, drop_ratio, delayMin, delayAvg, delayStddev)
        topo = SingleSwitchTopo(i)
        net = Mininet(topo=topo, host=CPULimitedHost, link=TCLink, autoStaticArp=True)
        host_list = []
        for j in range(i):
            host_list.append((net.hosts[j].IP(), net.get('h'+str(j+1))))
        net.start()

        """Measure performance"""
        # rps = []
        # latencies = {}
        for j in range(0, min(i // 2 + 1, 4)):
            res = singleBenchmark(fixedRps, 200, i, i + 1 - j, j, host_list, True) if j != 0 else singleBenchmark(fixedRps, 200, i, 0, 0, host_list, True)
            print('cluster size {}, q1 {}, q2 {}'.format(i, 0 if j == 0 else i - j + 1, j))
            print('Average delay:', res)

        """Record data"""
        # sleep(1)
        net.stop()
        # filename = 'latency_{}_{}.json'.format(i, fixedRps)
        # with open(filename, 'a') as f:
        #     f.write(json.dumps(latencies))
        # filename = "result_%d_%f" % (i, drop_ratio)
        # with open(filename, 'a') as f:
        #     f.write("RPS with cluster size = %d & drop ratio = %f\n" % (i, drop_ratio))
        #     f.write(str(rps)+"\n")

if __name__ == '__main__':
#    setLogLevel( 'info' )

    # set message loss rate %
    drop_ratio = [0]
    # drop_ratio = [0, 0.1, 1, 10]

    for i in drop_ratio:
        test_flexible_raft(i)
