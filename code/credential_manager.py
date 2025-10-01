"""
凭证管理模块

负责STS临时凭证的获取和管理，提供统一的凭证访问接口。
"""

import os
from typing import Optional
from dataclasses import dataclass
from alibabacloud_sts20150401.client import Client as StsClient
from alibabacloud_sts20150401 import models as sts_models
from alibabacloud_tea_openapi import models as open_api_models
from Tea.exceptions import TeaException

from config import STSConfig, config_manager


@dataclass
class STSCredentials:
    """STS临时凭证"""
    access_key_id: str
    access_key_secret: str
    security_token: str
    
    def is_valid(self) -> bool:
        """检查凭证是否有效"""
        return all([self.access_key_id, self.access_key_secret, self.security_token])


class CredentialError(Exception):
    """凭证相关错误"""
    pass


class STSCredentialManager:
    """STS凭证管理器"""
    
    def __init__(self, sts_config: Optional[STSConfig] = None):
        self.sts_config = sts_config or config_manager.get_sts_config()
        self._cached_credentials: Optional[STSCredentials] = None
    
    def get_credentials(self, force_refresh: bool = False) -> STSCredentials:
        """
        获取STS临时凭证
        
        Args:
            force_refresh: 是否强制刷新凭证
            
        Returns:
            STSCredentials: STS临时凭证
            
        Raises:
            CredentialError: 获取凭证失败时抛出
        """
        if not force_refresh and self._cached_credentials and self._cached_credentials.is_valid():
            return self._cached_credentials
        
        try:
            credentials = self._fetch_credentials()
            self._cached_credentials = credentials
            return credentials
        except Exception as e:
            raise CredentialError(f"获取STS凭证失败: {e}") from e
    
    def _fetch_credentials(self) -> STSCredentials:
        """从阿里云STS服务获取临时凭证"""
        # 获取主账号凭证
        access_key_id = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
        access_key_secret = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
        
        if not all([access_key_id, access_key_secret, self.sts_config.role_arn]):
            raise CredentialError(
                "缺少必需的环境变量: ALIBABA_CLOUD_ACCESS_KEY_ID, "
                "ALIBABA_CLOUD_ACCESS_KEY_SECRET 或 ALIBABA_CLOUD_ROLE_ARN"
            )
        
        # 创建STS客户端
        config = open_api_models.Config(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            endpoint=f'sts.{config_manager.get_sls_config().region}.aliyuncs.com'
        )
        sts_client = StsClient(config)
        
        # 构建AssumeRole请求
        assume_role_request = sts_models.AssumeRoleRequest(
            role_arn=self.sts_config.role_arn,
            role_session_name=self.sts_config.session_name,
            duration_seconds=self.sts_config.duration_seconds
        )
        
        try:
            response = sts_client.assume_role(assume_role_request)
            credentials_data = response.body.credentials
            
            return STSCredentials(
                access_key_id=credentials_data.access_key_id,
                access_key_secret=credentials_data.access_key_secret,
                security_token=credentials_data.security_token
            )
            
        except TeaException as e:
            error_msg = f"STS AssumeRole失败: {e.message}"
            if e.code:
                error_msg += f" (错误码: {e.code})"
            error_msg += "\n请检查: 1. 主账号AK是否正确; 2. 目标角色ARN是否正确; 3. 目标角色的信任策略是否已配置"
            raise CredentialError(error_msg) from e
        except Exception as e:
            raise CredentialError(f"获取STS凭证时发生未知错误: {e}") from e
    
    def clear_cache(self):
        """清除缓存的凭证"""
        self._cached_credentials = None
    
    def is_credentials_available(self) -> bool:
        """检查凭证是否可用"""
        try:
            credentials = self.get_credentials()
            return credentials.is_valid()
        except CredentialError:
            return False


# 全局凭证管理器实例
credential_manager = STSCredentialManager()
