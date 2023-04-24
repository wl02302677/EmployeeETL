from abc import ABCMeta, abstractmethod

# use ABCMeta compatible with Python 2 and Python 3
ABC = ABCMeta('ABC', (object,), {'__slots__': ()})


class ETL(ABC):
    """ Abstract ETL"""

    @abstractmethod
    def __init__(self, postgre):
        self.postgre = postgre

    @abstractmethod
    def verify(self, source_info):
        # verify the source data
        pass

    @abstractmethod
    def output(self, employee_info, salary_info):
        # transform to the output format
        pass
