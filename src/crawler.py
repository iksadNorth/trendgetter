from datetime import datetime

from abc import ABC, abstractmethod

class Crawler(ABC):
    """백필 기능에 사용하기 위해 일자별로 크롤링 할 수 있도록 추상 클래스로 정의.
    """
    @abstractmethod
    def scrap(self, created_at: datetime):
        pass

class YoutubeCrawler(Crawler):
    def scrap(self, created_at: datetime):
        pass

class InstagramCrawler(Crawler):
    def scrap(self, created_at: datetime):
        pass

class CommunityCrawler(Crawler):
    def scrap(self, created_at: datetime):
        pass