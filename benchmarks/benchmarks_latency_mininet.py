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

DEVNULL = open(os.devnull, 'wb')

START_PORT = 8000

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
        errRates.append(float(p.returncode) / 10000.0)
    avgRate = sum(errRates) / len(errRates)
    if delay:
        return avgRate
    return avgRate >= 0.9

class SingleSwitchTopo(Topo):
    """Single switch connected to n hosts."""
    def __init__(self, n=2, drop_ratio=0, lossy=False, **opts):
        Topo.__init__(self, **opts)
        switch = self.addSwitch('s1')

        for h in range(n):
            host = self.addHost('h%s' % (h + 1), cpu=.9 / n)
            self.addLink(host, switch, bw=100, delay='5ms', loss=drop_ratio, use_htb=True)

def test_latency_mininet():
    """Measure Latency vs cluster size of flexible Raft"""
    cluster_size = [i for i in range(3, 8, 2)]
    # test different phase 2 quorum size
    fixedRps = 50
    for i in cluster_size:
        """Create network"""
        # topo = SingleSwitchTopo(i, drop_ratio, delayMin, delayAvg, delayStddev)
        topo = SingleSwitchTopo(i)
        net = Mininet(topo=topo, host=CPULimitedHost, link=TCLink, autoStaticArp=True)
        host_list = []
        for j in range(i):
            host_list.append((net.hosts[j].IP(), net.get('h' + str(j + 1))))
        net.start()

        """Measure performance"""
        for j in range(0, min(i // 2 + 1, 4)):
            res = singleBenchmark(fixedRps, 10, i, i + 1 - j, j, host_list, delay=True) if j != 0 else singleBenchmark(
                fixedRps, 10, i, 0, 0, host_list, delay=True)
            print('cluster size {}, q1 {}, q2 {}'.format(i, 0 if j == 0 else i - j + 1, j))
            print('Average delay:', res)


        """Stop network"""
        net.stop()


if __name__ == '__main__':
    test_latency_mininet()