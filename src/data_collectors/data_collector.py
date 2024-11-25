from abc import ABC, abstractmethod

import pandas as pd


class DataCollector(ABC):
    """
    Abstract base class for all data collectors. Any data collector should implement the collect method.
    """

    @abstractmethod
    def collect(self, datetime) -> pd.DataFrame:
        """
        Collects data and stores it.

        Returns:
            pd.DataFrame: The collected data.
        """
        pass
