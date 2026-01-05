from abc import ABC, abstractmethod
from typing import List, Set, Optional
from kiwipiepy import Kiwi
from bs4 import BeautifulSoup

class QualityAssurance(ABC):
    """데이터 품질 검사 추상 클래스.
    """
    @abstractmethod
    def tokenize(self, text: str) -> List[str]:
        """텍스트를 토큰화하여 토큰 리스트 반환
        
        Args:
            text: 토큰화할 텍스트
            
        Returns:
            토큰 리스트
        """
        pass


# QualityAssurance 간단한 구현체 (나중에 고도화 예정)
class SimpleQualityAssurance(QualityAssurance):
    """간단한 토큰화 구현체 (임시)"""
    
    def tokenize(self, text: str) -> List[str]:
        """텍스트를 토큰화 (한국어 조사 제거 포함)
        
        Args:
            text: 토큰화할 텍스트
            
        Returns:
            토큰 리스트
        """
        if not text: return []
        
        # 간단한 공백 기준 토큰화 (나중에 고도화 예정)
        tokens = text.split()
        # 빈 토큰 제거
        tokens = [token.strip() for token in tokens if token.strip()]
        return tokens


class KiwiTokenizerQualityAssurance(QualityAssurance):
    """KiwiTokenizer를 사용한 토큰화 구현체 (불용어 제거 포함)"""
    
    def __init__(self, stopwords: Optional[Set[str]] = None, min_length: int = 1):
        """
        Args:
            stopwords: 불용어 집합 (None이면 기본 불용어 사용)
            min_length: 최소 토큰 길이 (기본값: 1)
        """
        self.kiwi = Kiwi()
        self.stopwords = stopwords or self._get_default_stopwords()
        self.min_length = min_length
        
        # 제외할 품사 (조사, 어미, 기호, 접미사만 - 외국어, 숫자는 통과)
        self.exclude_pos = {'J', 'E', 'S', 'X'}  # 조사, 어미, 기호, 접미사
    
    def _get_default_stopwords(self) -> Set[str]:
        """기본 불용어 리스트 반환"""
        return {
            '이', '가', '을', '를', '에', '의', '와', '과', '도', '로', '으로',
            '에서', '에게', '께', '한테', '에게서', '로부터', '처럼', '만큼',
            '하고', '와', '과', '그리고', '또한', '또', '그런데', '하지만',
            '그러나', '그래서', '그러므로', '따라서', '그럼', '그렇다면',
            '것', '거', '게', '건', '걸', '껏', '께', '께서',
            '수', '때', '곳', '데', '줄', '뿐', '밖에', '만',
            '이', '그', '저', '이것', '그것', '저것', '이런', '그런', '저런',
            '이렇게', '그렇게', '저렇게', '이만큼', '그만큼', '저만큼',
            '있다', '없다', '하다', '되다', '이다', '아니다',
            '되다', '돼다', '되', '돼', '되어', '돼어',
            '안', '못', '않', '말', '좀', '정도', '쯤'
        }
    
    def tokenize(self, text: str) -> List[str]:
        """텍스트를 토큰화하고 불용어 제거
        
        Args:
            text: 토큰화할 텍스트
            
        Returns:
            불용어가 제거된 토큰 리스트
        """
        if not text or not text.strip(): return []
        
        # 형태소 분석 (반환 형식: List[Tuple[List[Token], float]])
        morphs = self.kiwi.analyze(text)
        
        tokens = []
        for sent, _ in morphs:  # sent는 Token 리스트, _는 점수
            for token in sent:
                word, pos = token.form, token.tag  # 형태소
                
                # 품사 필터링 (조사, 어미, 접미사 제외)
                # 기호(S)는 제외하되, 수사(SN)와 외국어(SL)는 통과
                if pos.startswith(tuple(self.exclude_pos)) and pos not in ('SN', 'SL'): continue
                
                # 명사, 동사, 형용사, 부사, 외국어(SL), 숫자(SN) 추출
                if not pos.startswith(('N', 'V', 'A', 'M', 'SL', 'SN')): continue
                
                # 불용어 제거는 한글에만 적용
                if self._is_hangul(word) and word in self.stopwords: continue
                
                # 길이 필터링
                if len(word) < self.min_length: continue
                
                tokens.append(word.strip())
        return tokens
    
    def _is_hangul(self, text: str) -> bool:
        """텍스트가 한글로만 구성되어 있는지 확인
        
        Args:
            text: 확인할 텍스트
            
        Returns:
            한글로만 구성되어 있으면 True, 아니면 False
        """
        if not text:
            return False
        
        for char in text:
            # 한글 유니코드 범위
            # 완성형 한글: AC00-D7AF (가-힣)
            # 자모: 1100-11FF (초성/중성/종성)
            # 호환 자모: 3130-318F
            code = ord(char)
            if not (0xAC00 <= code <= 0xD7AF or  # 완성형 한글
                    0x1100 <= code <= 0x11FF or  # 자모
                    0x3130 <= code <= 0x318F):   # 호환 자모
                return False
        return True
    
    def remove_html_tags(self, text: str) -> str:
        """HTML 태그를 제거하고 텍스트만 추출
        
        BeautifulSoup을 사용하여 HTML 태그를 제거하되,
        영어 및 기타 텍스트는 보존합니다.
        
        Args:
            text: HTML 태그가 포함될 수 있는 텍스트
            
        Returns:
            HTML 태그가 제거된 텍스트
        """
        if not text:
            return ""
        
        # BeautifulSoup을 사용하여 HTML 태그 제거
        soup = BeautifulSoup(text, 'html.parser')
        # get_text()로 모든 태그를 제거하고 텍스트만 추출
        # strip=True로 앞뒤 공백 제거, separator=' '로 태그 사이 공백 처리
        cleaned_text = soup.get_text(separator=' ', strip=True)
        
        return cleaned_text


if __name__ == '__main__':
    # 테스트 코드
    print("=" * 60)
    print("KiwiTokenizerQualityAssurance 테스트")
    print("=" * 60)
    
    # 테스트 인스턴스 생성
    qa = KiwiTokenizerQualityAssurance(min_length=2)
    
    # HTML 태그 제거 테스트
    print("\n" + "=" * 60)
    print("HTML 태그 제거 테스트")
    print("=" * 60)
    
    html_test_cases = [
        "<p>안녕하세요 <strong>오늘</strong> 날씨가 좋네요</p>",
        "<div>이것은 <span>테스트</span> 문장입니다.</div>",
        "<a href='#'>링크 텍스트</a>와 일반 텍스트",
        "Python <code>프로그래밍</code>과 Java 개발",
        "<script>alert('test')</script>안녕하세요",
        "<style>.test { color: red; }</style>테스트",
        "일반 텍스트만 있는 경우",
        "<br/>줄바꿈 태그<br/>테스트",
        "<h1>제목</h1><p>본문</p>",
        "Mixed <b>English</b> and 한글 텍스트",
        "",
        "   ",
    ]
    
    for i, test_html in enumerate(html_test_cases, 1):
        print(f"\n[HTML 태그 제거 테스트 {i}]")
        print(f"입력: {repr(test_html)}")
        cleaned = qa.remove_html_tags(test_html)
        print(f"태그 제거 후: {repr(cleaned)}")
        tokens = qa.tokenize(cleaned)
        print(f"토큰화 결과: {tokens}")
        print(f"토큰 수: {len(tokens)}")
    
    # 일반 토큰화 테스트
    print("\n" + "=" * 60)
    print("일반 토큰화 테스트")
    print("=" * 60)
    
    test_cases = [
        "안녕하세요 오늘 날씨가 정말 좋네요",
        "이것은 테스트 문장입니다. 불용어가 제거되어야 합니다.",
        "그리고 또한 하지만 그래서 그러므로",
        "나는 오늘 학교에 가서 공부를 했다",
        "빈 문자열 테스트",
        "",
        "   ",
        "한국어와 영어가 섞인 Mixed Text 123",
        "Python 프로그래밍과 Java 개발",
        "2024년 12월 28일 날짜",
        "가격은 1000원이고 할인율은 50%입니다",
        "조사와 어미가 포함된 문장이에요",
        "ㅋㅋㅋㅋ ㅋㅋ ㅋㅋㅋㅋㅋ",
    ]
    
    for i, test_text in enumerate(test_cases, 1):
        print(f"\n[테스트 {i}]")
        print(f"입력: {repr(test_text)}")
        tokens = qa.tokenize(test_text)
        print(f"출력: {tokens}")
        print(f"토큰 수: {len(tokens)}")
    
    print("\n" + "=" * 60)
    print("테스트 완료!")
    print("=" * 60)
