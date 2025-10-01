"""
分析引擎包

提供错误分析和延迟分析的统一接口。
"""

# 导入共享模型
from .shared_models import AnalysisResult

# 导入分析器
from .error_analyzer import ErrorAnalyzer, error_analyzer
from .latency_analyzer import LatencyAnalyzer, latency_analyzer

# 定义包的公共接口
__all__ = [
    # 数据模型
    'AnalysisResult',
    
    # 分析器类
    'ErrorAnalyzer',
    'LatencyAnalyzer',
    
    # 分析器实例
    'error_analyzer',
    'latency_analyzer',
]

# 包版本信息
__version__ = "1.0.0"
__author__ = "Tianchi Analysis Team"
__description__ = "Root cause analysis engine for error and latency issues"
