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
    avgRPS = sum(num_success) / 30.0
    print('average RPS:', avgRPS)
    return avgRPS


class SingleSwitchTopo(Topo):
    """Single switch connected to n hosts."""
    def __init__(self, n=2, drop_ratio=0, delayMin=0.0, delayAvg=0.0, delayStddev=0.0, lossy=False, **opts):
        Topo.__init__(self, **opts)
        self.__delayMin = math.ceil(delayMin)
        self.__delayAvg = delayAvg
        self.__delayStddev = delayStddev
        switch = self.addSwitch('s1')

        for h in range(n):
            randDelay = str(self.__getRandomDelay()) + 'ms'
            host = self.addHost('h%s' % (h + 1), cpu=.9 / n)
            self.addLink(host, switch, bw=10, delay='5ms', loss=drop_ratio, use_htb=True)

    def __getRandomDelay(self):
        delay = math.ceil(random.gauss(self.__delayAvg, self.__delayStddev))
        if delay < self.__delayMin:
            return self.__delayMin
        return delay


def test_flexible_raft(drop_ratio):
    """Measure RPS vs cluster size of flexible Raft"""
    delayMin = 13.640
    delayAvg = 20.822
    delayStddev = 24.018
    cluster_size = [i for i in range(3, 10, 2)]

    for i in cluster_size:
        """Create network"""
        topo = SingleSwitchTopo(i, drop_ratio, delayMin, delayAvg, delayStddev)
        net = Mininet(topo=topo, link=TCLink, autoStaticArp=True)
        host_list = []
        for j in range(i):
            host_list.append((net.hosts[j].IP(), net.get('h'+str(j+1))))
        net.start()

        """Measure performance"""
        rps = []
        for j in range(0, min(i//2+1, 4)):
            res = singleBenchmark(i, i + 1 - j, j, drop_ratio) if j != 0 else singleBenchmark(i, 0, 0, drop_ratio)
            rps.append(res)

        """Record data"""
        sleep(1.0)
        net.stop()
        filename = "result_%d_%f" % (i, drop_ratio)
        with open(filename, 'a') as f:
            f.write("RPS with cluster size = %d & drop ratio = %f\n" % (i, drop_ratio))
            f.write(str(rps)+"\n")


if __name__ == '__main__':
    # setLogLevel( 'info' )

    # set message loss rate %
    drop_ratio = [0, 0.1, 0.5]

    for i in drop_ratio:
        test_flexible_raft(i)
