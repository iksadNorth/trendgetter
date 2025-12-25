# 타임스탬프 마이크로 단위 반환 함수
from datetime import datetime, timezone, timedelta
from typing import Optional

def get_timestamp_micro() -> int:
    current_time = datetime.now(timezone.utc)
    return int(current_time.timestamp() * 1000000)

def normalize_datetime(date_value) -> datetime:
    """다양한 형식의 날짜 값을 datetime 객체로 변환
    
    Args:
        date_value: 변환할 날짜 값 (datetime, str, Proxy 객체 등)
        
    Returns:
        datetime 객체
    """
    if isinstance(date_value, datetime):
        return date_value
    
    if isinstance(date_value, str):
        return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
    
    # Proxy 객체나 다른 타입인 경우 ISO 형식 문자열로 변환 후 파싱
    date_str = date_value.isoformat() if hasattr(date_value, 'isoformat') else str(date_value)
    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))

def normalize_created_at(created_at) -> Optional[datetime]:
    """created_at 값을 ISO 포맷 datetime으로 변환
    
    'YYYY.MM.DD HH:MM' 형식을 먼저 시도하고, 실패하면 ISO 형식을 시도합니다.
    
    Args:
        created_at: 변환할 created_at 값 (datetime, str 등)
        
    Returns:
        변환된 datetime 객체 또는 None
    """
    if not created_at:
        return None
    
    if isinstance(created_at, datetime):
        return created_at
    
    if not isinstance(created_at, str):
        return None
    
    try:
        return datetime.strptime(created_at, '%Y.%m.%d %H:%M')
    except ValueError:
        pass
    
    try:
        return datetime.fromisoformat(created_at.replace('Z', '+00:00'))
    except ValueError:
        return None

def get_week_range(execution_date: datetime) -> tuple[datetime, datetime]:
    """해당 주의 월요일부터 execution_date까지의 범위를 반환
    
    Args:
        execution_date: 기준 날짜
        
    Returns:
        (start_time, end_time) 튜플
        - start_time: 해당 주의 월요일 00:00:00
        - end_time: execution_date (명시적으로 datetime 객체로 변환)
    """
    # Proxy 객체나 문자열을 실제 datetime으로 변환
    execution_date = normalize_datetime(execution_date)
    
    # 해당 주의 월요일 계산 (weekday: 월요일=0, 일요일=6)
    days_to_monday = execution_date.weekday()
    start_time = (execution_date - timedelta(days=days_to_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # end_time을 명시적으로 새로운 datetime 객체로 생성 (Proxy 방지)
    end_time = datetime(
        year=execution_date.year,
        month=execution_date.month,
        day=execution_date.day,
        hour=execution_date.hour,
        minute=execution_date.minute,
        second=execution_date.second,
        microsecond=execution_date.microsecond,
        tzinfo=execution_date.tzinfo
    )
    
    return start_time, end_time
