"""FastAPI 서버 클라이언트 인터페이스"""
import requests
from typing import Optional, Dict, Any
from abc import ABC
from airflow.hooks.base import BaseHook


class BaseAPIClient(ABC):
    """FastAPI 서버 클라이언트 기본 클래스"""
    
    def __init__(
        self, 
        conn_id: str,
        timeout: int = 30
    ):
        """
        Args:
            conn_id: Airflow Connection ID (필수)
            timeout: 요청 타임아웃 (초)
        """
        if not conn_id:
            raise ValueError("conn_id는 필수입니다. Airflow Connection ID를 제공해야 합니다.")
        
        self.conn_id = conn_id
        self.timeout = timeout
        self.session = requests.Session()
        
        # Connection 정보 가져오기
        self.base_url = self._get_base_url_from_connection(conn_id)
        self._setup_session_from_connection(conn_id)
    
    def _get_base_url_from_connection(self, conn_id: str) -> str:
        """Airflow Connection에서 base_url 가져오기"""
        try:
            conn = BaseHook.get_connection(conn_id)

            scheme = conn.conn_type or "http"
            host = conn.host
            port = f":{conn.port}" if conn.port else ""

            if not host:
                raise ValueError("host is empty")

            auth = ""
            if conn.login:
                auth = f"{conn.login}:{conn.password or ''}@"

            return f"{scheme}://{auth}{host}{port}"

        except Exception as e:
            raise ValueError(
                f"Airflow Connection '{conn_id}'을 가져올 수 없습니다: {e}"
            )
    
    def _setup_session_from_connection(self, conn_id: str):
        """Connection 정보로 세션 설정"""
        try:
            conn = BaseHook.get_connection(conn_id)
            
            # extra 필드에서 추가 설정 가져오기 (JSON 형식)
            if conn.extra:
                import json
                try:
                    extra = json.loads(conn.extra) if isinstance(conn.extra, str) else conn.extra
                    
                    # Authorization 헤더 설정
                    if 'authorization' in extra or 'token' in extra:
                        token = extra.get('authorization') or extra.get('token')
                        self.session.headers.update({
                            'Authorization': f"Bearer {token}" if not token.startswith('Bearer ') else token
                        })
                    
                    # 커스텀 헤더 설정
                    if 'headers' in extra:
                        self.session.headers.update(extra['headers'])
                except (json.JSONDecodeError, TypeError):
                    pass
            
            # 기본 인증 (login/password가 있으면)
            if conn.login and conn.password:
                self.session.auth = (conn.login, conn.password)
        except Exception as e:
            # Connection 설정 실패해도 계속 진행 (기본 세션 사용)
            pass
    
    
    def _request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None,
        json: Optional[Dict] = None,
        **kwargs
    ) -> requests.Response:
        """
        HTTP 요청 실행
        
        Args:
            method: HTTP 메서드 (GET, POST, PUT, DELETE 등)
            endpoint: API 엔드포인트 (예: '/api/v1/data')
            params: URL 쿼리 파라미터
            json: JSON 요청 본문
            **kwargs: requests 라이브러리의 추가 파라미터
            
        Returns:
            requests.Response 객체
        """
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.timeout,
                **kwargs
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise Exception(f"API 요청 실패 ({method} {url}): {str(e)}")
    
    def get(self, endpoint: str, params: Optional[Dict] = None, **kwargs) -> requests.Response:
        """GET 요청"""
        return self._request('GET', endpoint, params=params, **kwargs)
    
    def post(self, endpoint: str, json: Optional[Dict] = None, **kwargs) -> requests.Response:
        """POST 요청"""
        return self._request('POST', endpoint, json=json, **kwargs)
    
    def put(self, endpoint: str, json: Optional[Dict] = None, **kwargs) -> requests.Response:
        """PUT 요청"""
        return self._request('PUT', endpoint, json=json, **kwargs)
    
    def delete(self, endpoint: str, json: Optional[Dict] = None, **kwargs) -> requests.Response:
        """DELETE 요청"""
        return self._request('DELETE', endpoint, json=json, **kwargs)


class PanAPIClient(BaseAPIClient):
    """Pan FastAPI 서버 클라이언트"""
    
    def __init__(
        self, 
        conn_id: str = 'api-server-pan',
        timeout: int = 30
    ):
        """
        Args:
            conn_id: Airflow Connection ID (기본값: 'api-server-pan')
            timeout: 요청 타임아웃 (초)
        """
        super().__init__(conn_id=conn_id, timeout=timeout)
    
    # Pan 서버의 특정 엔드포인트를 위한 메서드들
    def run_side(
        self, 
        side_id: str, 
        param: Dict[str, Any] = dict()
    ) -> Dict[str, Any]:
        """Side 실행
        결과값으로 HTML값을 전달받음"""
        json_data = {
            "side_id": side_id,
            "param": param,
        }
        response = self.post('/api/v1/sessions', json=json_data)
        
        # Content-Type 확인하여 HTML인지 JSON인지 판단
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' in content_type:
            return {'html': response.text}
        else:
            return response.json()


class HermesAPIClient(BaseAPIClient):
    """Hermes FastAPI 서버 클라이언트"""
    
    def __init__(
        self, 
        conn_id: str = 'api-server-hermes',
        timeout: int = 30
    ):
        """
        Args:
            conn_id: Airflow Connection ID (기본값: 'api-server-hermes')
            timeout: 요청 타임아웃 (초)
        """
        super().__init__(conn_id=conn_id, timeout=timeout)
    
    # Hermes 서버의 특정 엔드포인트를 위한 메서드들
    def apply_terraform(
        self, 
        project_id: str, 
        variables: Dict[str, Any] = dict(), 
        message: str|None = None
    ) -> Dict[str, Any]:
        """Terraform 적용"""
        json_data = {
            "variables": variables,
            "message": message,
        }
        response = self.post(f'/api/v1/tfpjts/{project_id}', json=json_data)
        return response.json()
    
    def destroy_terraform(
        self, 
        project_id: str, 
        variables: Dict[str, Any] = dict(), 
        message: str|None = None
    ) -> Dict[str, Any]:
        """Terraform 제거"""
        json_data = {
            "variables": variables,
            "message": message,
        }
        response = self.delete(f'/api/v1/tfpjts/{project_id}', json=json_data)
        return response.json()
    
    # 필요에 따라 Hermes 서버의 다른 엔드포인트 메서드 추가

