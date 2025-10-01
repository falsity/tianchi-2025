"""
共享数据模型和工具函数

包含错误分析和延迟分析共同使用的数据结构和工具函数。
"""

import logging
from typing import List, Optional
from dataclasses import dataclass

# 设置日志记录器
logger = logging.getLogger(__name__)


@dataclass
class AnalysisResult:
    """分析结果"""

    root_causes: List[str]
    confidence: str
    evidence: bool
    error_message: Optional[str] = None

    def is_success(self) -> bool:
        """检查分析是否成功"""
        return self.root_causes and len(self.root_causes) > 0 and not self.error_message
