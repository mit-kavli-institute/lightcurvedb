"""
This module is to contain all multiprocessing locking mechanisms that
lightcurvedb uses for async queries.
"""

from multiprocessing import Semaphore

class QLPBarrier(object):
    """A shareable lock between multiple processes"""
    def __init__(self, n_parties):
        """__init__.

        Parameters
        ----------
        n_parties :
            The number of locking parties required to release the barrier.
        """
        self.parties = n_parties
        self.n_waiting = 0
        self.mutex = Semaphore(1)
        self.barrier = Semaphore(0)

    def wait(self):
        """wait.
        Lock on the QLPBarrier until released by the last thread.
        """
        self.mutex.acquire()
        self.n_waiting += 1
        self.mutex.release()

        # When the expected amount of parties are waiting
        # the last callee will allow barrier to release
        if self.n_waiting == self.parties: self.barrier.release()
        self.barrier.acquire()
        self.barrier.release()
