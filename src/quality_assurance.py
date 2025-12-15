from abc import ABC, abstractmethod

from model import ScrapedData

class QualityAssurance(ABC):
    """데이터 품질 검사 추상 클래스.
    """
    @abstractmethod
    def check(self, data: ScrapedData):
        pass
