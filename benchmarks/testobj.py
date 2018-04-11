from __future__ import print_function
import sys
import time
import random
from collections import defaultdict
sys.path.append("../")
from pysyncobj import SyncObj, replicated, SyncObjConf, FAIL_REASON


def parseParams(params):
    """Parse command line parameters."""
    if len(params) < 8:
        print('Usage: %s RPS requestSize quorumSize1 quorumSize2 selfHost:port partner1Host:port partner2Host:port ...' % sys.argv[0])
        sys.exit(-1)
    return int(float(params[1])), int(params[2]), int(params[3]), int(params[4]), float(params[5]), (params[6] if params[6] != 'readonly' else None), params[7:]


class TestObj(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs, quorumSize1=0, quorumSize2=0, drop_ratio=0.0):
        super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs, quorumSize1, quorumSize2, drop_ratio)
        self.__appliedCommands = 0

    @replicated
    def testMethod(self, value):
        self.__appliedCommands += 1

    def getNumCommandsApplied(self):
        return self.__appliedCommands


_g_sent = 0
_g_success = 0
_g_error = 0
_g_errors = defaultdict(int)


def clbck(res, err):
    global _g_error, _g_success
    if err == FAIL_REASON.SUCCESS:
        _g_success += 1
    else:
        _g_error += 1
        _g_errors[err] += 1


def getRandStr(l):
    f = '%0' + str(l) + 'x'
    return f % random.randrange(16 ** l)


if __name__ == '__main__':

    # Parse parameters
    numCommands, cmdSize, quorumSize1, quorumSize2, drop_ratio, selfAddr, partners = parseParams(sys.argv)
    maxCommandsQueueSize = int(0.9 * SyncObjConf().commandsQueueSize / len(partners))

    # Init a TestObj
    obj = TestObj(selfAddr, partners, quorumSize1, quorumSize2, drop_ratio)

    while obj._getLeader() is None:
        time.sleep(0.5)

    time.sleep(4.0)

    # Measure the system during its steady state
    startTime = time.time()
    while time.time() - startTime < 10.0:
        st = time.time()
        for i in range(0, numCommands):
            obj.testMethod(getRandStr(cmdSize), callback=clbck)
            _g_sent += 1
        delta = time.time() - st
        if delta > 1.0:
            sys.exit(0)
        else:
            time.sleep(1.0 - delta)

    time.sleep(1.0)

    successRate = float(_g_success) / float(_g_sent)
    # print('SUCCESS RATE:', successRate)

    # if successRate < 0.9:
    #     print('LOST RATE:', 1.0 - float(_g_success + _g_error) / float(_g_sent))
    #     print('ERRORS STATS: %d' % len(_g_errors))
    #     for err in _g_errors:
    #         print(err, float(_g_errors[err]) / float(_g_error))

    sys.exit(int(successRate * 100))
