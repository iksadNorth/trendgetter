from datetime import datetime


class YoutubeCrawler():
    def scrap(self, created_at: datetime):
        pass

class InstagramCrawler():
    def scrap(self, created_at: datetime):
        pass

class CommunityCrawler():
    def scrap_array_id(self, created_at: datetime):
        return ['9263344374', '9263492727', '9263440283']
    
    def scrap_article_data(self, article_id: str):
        return [
            {'src_pk': article_id, 'seq': '0', 'text': '여기까지 [LIT]임 ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ', 'created_at': '2025.12.10 22:58', 'created_by': '191735343'},
            {'src_pk': article_id, 'seq': '1', 'text': '왜 좋냐', 'created_at': '2025.12.10 23:02', 'created_by': '6998518866'},
            {'src_pk': article_id, 'seq': '2', 'text': '존나 힙하네', 'created_at': '2025.12.10 23:02', 'created_by': '8177365530'},
            {'src_pk': article_id, 'seq': '3', 'text': '좋은데 ㅋㅋ', 'created_at': '2025.12.10 23:02', 'created_by': '6904184348'},
            {'src_pk': article_id, 'seq': '4', 'text': '싹 긁어냈다매 이게 존나 빡세다ㅋㅋㅋㅋ', 'created_at': '2025.12.10 23:02', 'created_by': '3856208534'},
        ]