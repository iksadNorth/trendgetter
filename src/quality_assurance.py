from abc import ABC, abstractmethod
from typing import List

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
