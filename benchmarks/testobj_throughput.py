from __future__ import print_function
import sys
import time
import random
from collections import defaultdict
sys.path.append("../")
from pysyncobj import SyncObj, replicated, SyncObjConf, FAIL_REASON
from pysyncobj.batteries import ReplCounter, ReplDict


def parseParams(params):
    """Parse command line parameters."""
    if len(params) < 6:
        print('Usage: %s quorumSize1 quorumSize2 selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)
    return int(params[1]), int(params[2]), float(params[3]), (params[4] if params[4] != 'readonly' else None), params[5:]


def measure(argv):
    """Measure throughput in 15s"""

    # Parse parameters
    quorumSize1, quorumSize2, drop_ratio, selfAddr, partners = parseParams(argv)
    maxCommandsQueueSize = int(0.9 * SyncObjConf().commandsQueueSize / len(partners))

    # Init a TestObj
    counter1 = ReplCounter()
    obj = SyncObj(selfAddr, partners, quorumSize1, quorumSize2, drop_ratio, consumers=[counter1])

    while obj._getLeader() is None:
        time.sleep(0.5)
    time.sleep(4.0)
    count = 0
    startTime = time.time()
    while time.time() - startTime < 10.0:
        counter1.inc(sync=True)
    while time.time() - startTime < 60.0:
        counter1.inc(sync=True)
        count += 1
    while time.time() - startTime < 5.0:
        counter1.inc(sync=True)

    time.sleep(2.0)

    return count


if __name__ == '__main__':
    ret = measure(sys.argv)
    sys.exit(int(ret))
