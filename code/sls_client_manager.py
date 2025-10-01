"""
SLS客户端管理模块

负责SLS客户端的创建和管理，提供统一的日志查询接口。
"""

import logging
from typing import Optional
from aliyun.log import LogClient, GetLogsRequest
from Tea.exceptions import TeaException

from config import SLSConfig, config_manager
from credential_manager import CredentialError, credential_manager

# 设置日志记录器
logger = logging.getLogger(__name__)


class SLSClientError(Exception):
    """SLS客户端相关错误"""
    pass


class SLSClientManager:
    """SLS客户端管理器"""
    
    def __init__(self, sls_config: Optional[SLSConfig] = None):
        self.sls_config = sls_config or config_manager.get_sls_config()
        self._client: Optional[LogClient] = None
    
    def get_client(self, force_refresh: bool = False) -> LogClient:
        """
        获取SLS客户端
        
        Args:
            force_refresh: 是否强制刷新客户端
            
        Returns:
            LogClient: SLS客户端实例
            
        Raises:
            SLSClientError: 创建客户端失败时抛出
        """
        if not force_refresh and self._client is not None:
            return self._client
        
        try:
            # 获取STS凭证
            credentials = credential_manager.get_credentials(force_refresh)
            if not credentials.is_valid():
                raise SLSClientError("STS凭证无效")
            
            # 创建SLS客户端
            self._client = LogClient(
                endpoint=self.sls_config.endpoint,
                accessKeyId=credentials.access_key_id,
                accessKey=credentials.access_key_secret,
                securityToken=credentials.security_token
            )
            
            return self._client
            
        except CredentialError as e:
            raise SLSClientError(f"获取凭证失败: {e}") from e
        except Exception as e:
            raise SLSClientError(f"创建SLS客户端失败: {e}") from e
    
    def execute_query(self, query: str, start_time: int, end_time: int, 
                     limit: int = 1000) -> list:
        """
        执行SLS查询
        
        Args:
            query: SLS查询语句
            start_time: 开始时间戳
            end_time: 结束时间戳
            limit: 返回结果数量限制
            
        Returns:
            list: 查询结果列表
            
        Raises:
            SLSClientError: 查询失败时抛出
        """
        try:
            client = self.get_client()
            
            request = GetLogsRequest(
                project=self.sls_config.project_name,
                logstore=self.sls_config.logstore_name,
                query=query,
                fromTime=start_time,
                toTime=end_time,
                line=limit
            )
            
            response = client.get_logs(request)
            
            if response and response.get_count() > 0:
                return [log_item.get_contents() for log_item in response.get_logs()]
            else:
                return []
                
        except TeaException as e:
            error_msg = f"SLS查询失败: {e.message}"
            if e.code:
                error_msg += f" (错误码: {e.code})"
            error_msg += "\n请检查: 1. 临时凭证是否已过期; 2. 扮演的角色是否拥有对目标Project和Logstore的读权限"
            raise SLSClientError(error_msg) from e
        except Exception as e:
            raise SLSClientError(f"执行SLS查询时发生未知错误: {e}") from e
    
    def test_connection(self) -> bool:
        """
        测试SLS连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 首先测试凭证是否可用
            if not credential_manager.is_credentials_available():
                logger.error("❌ STS凭证不可用")
                return False
            
            # 尝试创建客户端
            client = self.get_client()
            if client is None:
                logger.error("❌ 无法创建SLS客户端")
                return False
            
            logger.info("✅ SLS客户端创建成功")
            return True
            
        except Exception as e:
            logger.error("❌ SLS连接测试失败: %s", e)
            return False
    
    def clear_client(self):
        """清除客户端缓存"""
        self._client = None
    
    def get_project_name(self) -> str:
        """获取项目名称"""
        return self.sls_config.project_name
    
    def get_logstore_name(self) -> str:
        """获取日志库名称"""
        return self.sls_config.logstore_name
    
    def get_region(self) -> str:
        """获取地域"""
        return self.sls_config.region


# 全局SLS客户端管理器实例
sls_client_manager = SLSClientManager()
