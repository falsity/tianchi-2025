"""
Span查找器模块

包含用于查找根因spans的各种查找器：
- ErrorSpanFinder: 错误分析中的span查找器
- LatencySpanFinder: 延迟分析中的span查找器
"""

from .error_span_finder import FindRootCauseSpans as ErrorSpanFinder
from .latency_span_finder import FindRootCauseSpansRT as LatencySpanFinder

__all__ = ['ErrorSpanFinder', 'LatencySpanFinder']
