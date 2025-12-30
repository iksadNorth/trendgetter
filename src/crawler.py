import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from src.utils import normalize_created_at


class YoutubeCrawler():
    def __init__(self, api_key: Optional[str] = None):
        """YouTube Data API v3 클라이언트 초기화
        
        Args:
            api_key: YouTube Data API 키 (None이면 YOUTUBE_API_KEY 환경 변수 사용)
        """
        load_dotenv()  # .env 파일 로드
        self.api_key = api_key or os.getenv('YOUTUBE_API_KEY')
        if not self.api_key:
            raise ValueError("YOUTUBE_API_KEY 환경 변수가 설정되지 않았습니다.")
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
    
    def scrap_array_id(self, created_at: datetime) -> List[str]:
        """한국 인기 동영상 상위 10개 영상 ID 가져오기
        
        Args:
            created_at: 실행 시간 (현재는 사용하지 않지만 인터페이스 호환성을 위해 유지)
            
        Returns:
            영상 ID 리스트 (최대 10개)
        """
        try:
            # 한국 인기 동영상 가져오기
            request = self.youtube.videos().list(
                part='id',
                chart='mostPopular',
                regionCode='KR',
                maxResults=10
            )
            response = request.execute()
            
            video_ids = [item['id'] for item in response.get('items', [])]
            return video_ids[:10]  # 최대 10개만 반환
            
        except Exception as e:
            print(f"trending videos 오류: {e}")
            return []
    
    def scrap_article_data(self, article_id: str) -> List[Dict[str, Any]]:
        """특정 영상의 댓글 가져오기
        
        Args:
            article_id: YouTube 영상 ID
            
        Returns:
            댓글 데이터 리스트 (스키마에 맞게 변환됨)
        """
        raw_data = []
        next_page_token = None
        max_results = 100
        
        while len(raw_data) < max_results:
            try:
                request = self.youtube.commentThreads().list(
                    part='snippet,replies',
                    videoId=article_id,
                    maxResults=min(100, max_results - len(raw_data)),  # 한 번에 최대 100개
                    pageToken=next_page_token,
                    order='relevance'  # 관련성 순으로 정렬
                )
                response = request.execute()
                
                # 댓글 파싱
                for item in response.get('items', []):
                    # 최상위 댓글
                    top_level_comment = item['snippet']['topLevelComment']['snippet']
                    comment_data = {
                        'src_pk': article_id,
                        'src_id': 'youtube',
                        'comment_id': item['snippet']['topLevelComment']['id'],
                        'text': top_level_comment['textDisplay'],
                        'created_at': top_level_comment['publishedAt'],
                        'created_by': top_level_comment.get('authorChannelId', {}).get('value', top_level_comment.get('authorDisplayName', 'unknown'))
                    }
                    raw_data.append(comment_data)
                    
                    # 답글 (replies) 처리
                    if 'replies' not in item: continue
                    for reply in item['replies']['comments']:
                        reply_snippet = reply['snippet']
                        reply_data = {
                            'src_pk': article_id,
                            'src_id': 'youtube',
                            'comment_id': reply['id'],
                            'text': reply_snippet['textDisplay'],
                            'created_at': reply_snippet['publishedAt'],
                            'created_by': reply_snippet.get('authorChannelId', {}).get('value', reply_snippet.get('authorDisplayName', 'unknown'))
                        }
                        raw_data.append(reply_data)
                
                # 다음 페이지 토큰 확인
                next_page_token = response.get('nextPageToken')
                if not next_page_token: break
            
            except HttpError as e:
                if e.resp.status == 403:
                    error_str = str(e).lower()
                    is_comments_disabled = 'commentsdisabled' in error_str or 'disabled comments' in error_str
                    if is_comments_disabled:
                        return []
                    else:
                        print(f"comments 오류 (403): {e}")
                        return []
                elif e.resp.status == 404:
                    return []
                else:
                    print(f"comments 오류 ({e.resp.status}): {e}")
                    if not next_page_token:
                        break
                    continue
            
            except Exception as e:
                print(f"comments 오류: {e}")
                if not next_page_token:
                    break
                continue
        
        # created_at 정규화
        for doc in raw_data:
            if 'created_at' not in doc: continue
            normalized = normalize_created_at(doc['created_at'])
            if normalized is None: continue
            doc['created_at'] = normalized  # type: ignore
        return raw_data


if __name__ == '__main__':
    from datetime import datetime
    
    print("=" * 60)
    print("YoutubeCrawler 테스트")
    print("=" * 60)
    
    try:
        # YoutubeCrawler 인스턴스 생성
        print("\n[1] YoutubeCrawler 초기화 중...")
        crawler = YoutubeCrawler()
        print("✅ 초기화 완료")
        
        # Trending 영상 ID 가져오기
        print("\n[2] 한국 인기 동영상 상위 10개 가져오기...")
        video_ids = crawler.scrap_array_id(created_at=datetime.now())
        print(f"✅ {len(video_ids)}개 영상 ID 수집 완료")
        print(f"   영상 ID 목록: {video_ids[:5]}..." if len(video_ids) > 5 else f"   영상 ID 목록: {video_ids}")
        
        if not video_ids:
            print("⚠️  영상 ID를 가져올 수 없습니다. API 키를 확인하세요.")
            exit(1)
        
        # 첫 번째 영상의 댓글 가져오기
        print(f"\n[3] 첫 번째 영상 (ID: {video_ids[0]})의 댓글 가져오기...")
        comments = crawler.scrap_article_data(article_id=video_ids[0])
        print(f"✅ {len(comments)}개 댓글 수집 완료")
        
        if comments:
            print("\n[4] 수집된 댓글 샘플 (최대 3개):")
            for i, comment in enumerate(comments[:3], 1):
                print(f"\n   댓글 {i}:")
                print(f"   - comment_id: {comment.get('comment_id')}")
                print(f"   - text: {comment.get('text', '')[:50]}..." if len(comment.get('text', '')) > 50 else f"   - text: {comment.get('text', '')}")
                print(f"   - created_at: {comment.get('created_at')}")
                print(f"   - created_by: {comment.get('created_by')}")
        else:
            print("⚠️  댓글을 가져올 수 없습니다.")
        
        print("\n" + "=" * 60)
        print("테스트 완료!")
        print("=" * 60)
        
    except ValueError as e:
        print(f"\n❌ 오류: {e}")
        print("   YOUTUBE_API_KEY 환경 변수를 설정하세요.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()


class InstagramCrawler():
    def scrap_array_id(self, created_at: datetime):
        return ['9263344374', '9263492727', '9263440283']
    
    def scrap_article_data(self, article_id: str):
        raw_data = [
            {'src_pk': article_id, 'src_id': 'instagram', 'comment_id': '0', 'text': '여기까지 [LIT]임 ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ', 'created_at': '2025.12.28 12:58', 'created_by': '191735343'},
            {'src_pk': article_id, 'src_id': 'instagram', 'comment_id': '1', 'text': '왜 좋냐', 'created_at': '2025.12.28 13:02', 'created_by': '6998518866'},
            {'src_pk': article_id, 'src_id': 'instagram', 'comment_id': '2', 'text': '존나 힙하네', 'created_at': '2025.12.28 13:02', 'created_by': '8177365530'},
            {'src_pk': article_id, 'src_id': 'instagram', 'comment_id': '3', 'text': '좋은데 ㅋㅋ', 'created_at': '2025.12.28 13:02', 'created_by': '6904184348'},
            {'src_pk': article_id, 'src_id': 'instagram', 'comment_id': '4', 'text': '싹 긁어냈다매 이게 존나 빡세다ㅋㅋㅋㅋ', 'created_at': '2025.12.28 13:02', 'created_by': '3856208534'},
        ]
        for doc in raw_data:
            if 'created_at' not in doc: continue
            normalized = normalize_created_at(doc['created_at'])
            if normalized is None: continue
            doc['created_at'] = normalized  # type: ignore
        return raw_data


class CommunityCrawler():
    def scrap_array_id(self, created_at: datetime):
        return []
    
    def scrap_article_data(self, article_id: str):
        raw_data = []
        for doc in raw_data:
            if 'created_at' not in doc: continue
            normalized = normalize_created_at(doc['created_at'])
            if normalized is None: continue
            doc['created_at'] = normalized  # type: ignore
        return raw_data
