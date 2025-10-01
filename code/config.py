"""
配置管理模块

统一管理所有硬编码的配置项，提供类型安全的配置访问接口。
"""

import os
from dataclasses import dataclass
from typing import List, Optional
from enum import Enum


class AnalysisType(Enum):
    """分析类型枚举"""
    ERROR = "error"
    LATENCY = "latency"


@dataclass
class SLSConfig:
    """SLS配置"""
    project_name: str
    logstore_name: str
    region: str
    endpoint: str
    
    @classmethod
    def from_env(cls) -> 'SLSConfig':
        """从环境变量创建配置"""
        return cls(
            project_name=os.getenv('SLS_PROJECT_NAME', 'proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao'),
            logstore_name=os.getenv('SLS_LOGSTORE_NAME', 'logstore-tracing'),
            region=os.getenv('SLS_REGION', 'cn-qingdao'),
            endpoint=f"{os.getenv('SLS_REGION', 'cn-qingdao')}.log.aliyuncs.com"
        )


@dataclass
class STSConfig:
    """STS配置"""
    role_arn: str
    session_name: str
    duration_seconds: int = 3600
    
    @classmethod
    def from_env(cls) -> 'STSConfig':
        """从环境变量创建配置"""
        return cls(
            role_arn=os.getenv('ALIBABA_CLOUD_ROLE_ARN', 'acs:ram::1672753017899339:role/tianchi-user-a'),
            session_name=os.getenv('ALIBABA_CLOUD_ROLE_SESSION_NAME', 'my-sls-access'),
            duration_seconds=int(os.getenv('STS_DURATION_SECONDS', '3600'))
        )


@dataclass
class AnalysisConfig:
    """分析配置"""
    error_traces_limit: int = 2000
    high_rt_traces_limit: int = 2000
    traces_for_avg_rt: int = 3000
    percent_95: float = 0.95
    max_duration: int = 5 * 10**6  # 5秒
    duration_threshold: int = 2000000000  # 2秒
    
    @classmethod
    def from_env(cls) -> 'AnalysisConfig':
        """从环境变量创建配置"""
        return cls(
            error_traces_limit=int(os.getenv('ERROR_TRACES_LIMIT', '2000')),
            high_rt_traces_limit=int(os.getenv('HIGH_RT_TRACES_LIMIT', '2000')),
            traces_for_avg_rt=int(os.getenv('TRACES_FOR_AVG_RT', '3000')),
            percent_95=float(os.getenv('PERCENT_95', '0.95')),
            max_duration=int(os.getenv('MAX_DURATION', str(5 * 10**6))),
            duration_threshold=int(os.getenv('DURATION_THRESHOLD', '2000000000'))
        )


@dataclass
class AppConfig:
    """应用程序配置"""
    sls: SLSConfig
    sts: STSConfig
    analysis: AnalysisConfig
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        """从环境变量创建完整配置"""
        return cls(
            sls=SLSConfig.from_env(),
            sts=STSConfig.from_env(),
            analysis=AnalysisConfig.from_env()
        )


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or AppConfig.from_env()
    
    def get_sls_config(self) -> SLSConfig:
        """获取SLS配置"""
        return self.config.sls
    
    def get_sts_config(self) -> STSConfig:
        """获取STS配置"""
        return self.config.sts
    
    def get_analysis_config(self) -> AnalysisConfig:
        """获取分析配置"""
        return self.config.analysis
    
    def validate_required_env_vars(self) -> List[str]:
        """验证必需的环境变量"""
        required_vars = [
            'ALIBABA_CLOUD_ACCESS_KEY_ID',
            'ALIBABA_CLOUD_ACCESS_KEY_SECRET',
            'ALIBABA_CLOUD_ROLE_ARN'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        return missing_vars
    
    def is_config_valid(self) -> bool:
        """检查配置是否有效"""
        return len(self.validate_required_env_vars()) == 0


# 全局配置实例
config_manager = ConfigManager()
