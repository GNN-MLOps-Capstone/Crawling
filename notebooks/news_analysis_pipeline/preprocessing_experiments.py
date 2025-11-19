from abc import ABC, abstractmethod
import pandas as pd
from news_data_cleaning import clean_text_data

class BasePreprocessor(ABC):
    """
    Abstract base class for preprocessing strategies.
    All preprocessing experiments must inherit from this class and implement the `process` method.
    """
    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the input dataframe and return the cleaned dataframe.
        """
        pass

class BaselinePreprocessor(BasePreprocessor):
    """
    The baseline preprocessing strategy.
    Uses the logic defined in `news_data_cleaning.py`.
    """
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Running Baseline Preprocessor...")
        # We simply delegate to the existing function
        return clean_text_data(df)

class ExperimentalPreprocessorV1(BasePreprocessor):
    """
    Example of an experimental preprocessing strategy.
    You can implement different logic here (e.g., different regex, different filtering).
    """
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        print("Running Experimental Preprocessor V1...")
        # Example: Maybe we want to be less strict with sports news in this experiment
        # For now, let's just call the baseline but print a message, 
        # or you can copy-paste logic from clean_text_data and modify it.
        
        # For demonstration, let's say V1 is identical to Baseline but we might change it later
        return clean_text_data(df)

def get_preprocessor(name: str) -> BasePreprocessor:
    """
    Factory function to get a preprocessor by name.
    """
    if name == "baseline":
        return BaselinePreprocessor()
    elif name == "experiment_v1":
        return ExperimentalPreprocessorV1()
    else:
        raise ValueError(f"Unknown preprocessor: {name}")
