from __future__ import print_function
import sys
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
import matplotlib.pyplot as plt


DEVNULL = open(os.devnull, 'wb')

START_PORT = 4321
MIN_RPS = 10
MAX_RPS = 20000


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


def singleBenchmark(requestsPerSecond, requestSize, numNodes, quorumSize1, quorumSize2,
                    IPlist, numNodesReadonly=0, delay=False):
    """Execute benchmark."""
    rpsPerNode = requestsPerSecond / (numNodes + numNodesReadonly)
    cmd = [sys.executable, 'testobj_delay.py' if delay else 'testobj.py', str(rpsPerNode), str(requestSize),
           str(quorumSize1), str(quorumSize2), str(drop_ratio)]
    processes = []
    allAddrs = []
    for i in range(numNodes):
        allAddrs.append('%s:%d' % (IPlist[i], START_PORT + i))
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
    # print('average success rate:', avgRate)
    if delay:
        return avgRate
    return avgRate >= 0.9


def vote(c, requestSize, numNodes, quorumSize1, quorumSize2, IPlist):
    t1 = t2 = 0
    for i in range(3):
        ret = singleBenchmark(c, requestSize, numNodes, quorumSize1, quorumSize2, IPlist)
        if ret:
            t1 += 1
        else:
            t2 += 1
    return t1 > t2


def doDetectMaxRps(requestSize, numNodes, quorumSize1, quorumSize2, IPlist):
    """Measure max RPS with binary search"""
    a = MIN_RPS
    b = MAX_RPS
    numIt = 0
    while b - a > MIN_RPS:
        c = a + (b - a) / 2
        res = vote(c, requestSize, numNodes, quorumSize1, quorumSize2, IPlist)
        if res:
            a = c
        else:
            b = c
        print('subiteration %d, current max %d' % (numIt, a))
        numIt += 1
    return a


# @memoize('maxRpsCache.bin')
def detectMaxRps(requestSize, numNodes, quorumSize1, quorumSize2, IPlist):
    """Measure max RPS three times and use median as result."""
    results = []
    for i in range(0, 1):
        res = doDetectMaxRps(requestSize, numNodes, quorumSize1, quorumSize2, IPlist)
        print('iteration %d, current max %d' % (i, res))
        results.append(res)
    return sorted(results)[len(results) // 2]


def measure_RPS_vs_Clustersize(q2=0, drop_ratio=0.0):
    """Measure max RPS as a function of cluster size."""
    cluster_size = [i for i in range(3, 8, 2)]
    rps = []
    for i in cluster_size:
        res = detectMaxRps(200, i, i+1-q2, q2, drop_ratio) if q2 != 0 else detectMaxRps(200, i, 0, 0, drop_ratio)
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


def printUsage():
    print('Usage: %s mode(delay/rps/custom)' % sys.argv[0])
    sys.exit(-1)


class SingleSwitchTopo(Topo):
    """Single switch connected to n hosts."""
    def __init__(self, n=2, drop_ratio=0, lossy=False, **opts):
        Topo.__init__(self, **opts)
        switch = self.addSwitch('s1')
        for h in range(n):
            # Each host gets 50%/n of system CPU
            host = self.addHost('h%s' % (h + 1), cpu=.9 / n)
            # 10 Mbps, 5ms delay, no packet loss
            self.addLink(host, switch, bw=10, delay='5ms', loss=drop_ratio, use_htb=True)


def test_flexible_raft(drop_ratio):
    """Measure RPS vs cluster size of flexible Raft"""
    cluster_size = [i for i in range(3, 10, 2)]
    # test different phase 2 quorum size
    for i in cluster_size:
        """Create network"""
        topo = SingleSwitchTopo(i, drop_ratio)
        net = Mininet(topo=topo, host=CPULimitedHost, link=TCLink, autoStaticArp=True)
        IPlist = []
        for j in range(i):
            IPlist.append(net.hosts[j].IP())
        net.start()

        """Measure performance"""
        rps = []
        for j in range(0, 4):
            res = detectMaxRps(200, i, i + 1 - j, j, IPlist) if j != 0 else detectMaxRps(200, i, 0, 0, IPlist)
            rps.append(res)

        """Record data"""
        sleep(1)
        net.stop()
        filename = "result_%d_%f" % (i, drop_ratio)
        with open(filename, 'a') as f:
            f.write("RPS with cluster size = %d & drop ratio = %f\n" % (i, drop_ratio))
            f.write(str(rps))


if __name__ == '__main__':

    if len(sys.argv) != 2:
        printUsage()

    mode = sys.argv[1]

    # set quorum size for phase 2: 0 -> normal Raft, >0 -> flexible Raft
    quorumSize2 = 0

    # set message loss rate
    drop_ratio = [0, 0.1, 1, 5]

    if mode == 'delay':
        print('Average delay:', singleBenchmark(50, 10, 5, delay=True))
    elif mode == 'rps':
        for i in drop_ratio:
            test_flexible_raft(i)
        # measure_RPS_vs_Clustersize(quorumSize2, drop_ratio)
        # measure_RPS_vs_Requestsize()

    elif mode == 'custom':
        singleBenchmark(25000, 10, 3)
    else:
        printUsage()

