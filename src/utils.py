# 타임스탬프 마이크로 단위 반환 함수
from datetime import datetime, timezone

def get_timestamp_micro() -> int:
    current_time = datetime.now(timezone.utc)
    return int(current_time.timestamp() * 1000000)
