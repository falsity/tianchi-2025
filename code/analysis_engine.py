"""
分析引擎模块

统一管理错误分析和延迟分析的逻辑，提供简洁的分析接口。
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from config import config_manager
from sls_client_manager import sls_client_manager, SLSClientError
from credential_manager import CredentialError

# 导入现有的分析模块（保持原有文件不变）
from find_root_cause_spans_error import FindRootCauseSpans
from find_root_cause_spans_rt import FindRootCauseSpansRT

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


class AnalysisError(Exception):
    """分析相关错误"""
    pass


class RootCauseAnalyzer:
    """根因分析器"""
    
    def __init__(self):
        self.config = config_manager.get_analysis_config()
        self.sls_config = config_manager.get_sls_config()
    
    def analyze_error_root_cause(self, start_time: str, end_time: str, 
                                candidate_root_causes: List[str]) -> AnalysisResult:
        """
        分析错误根因
        
        Args:
            start_time: 开始时间 (YYYY-MM-DD HH:MM:SS)
            end_time: 结束时间 (YYYY-MM-DD HH:MM:SS)
            candidate_root_causes: 候选根因列表
            
        Returns:
            AnalysisResult: 分析结果
        """
        try:
            logger.info("--- 开始执行错误根因分析 ---")
            
            # 1. 创建SLS客户端
            sls_client = sls_client_manager.get_client()
            
            # 2. 创建根因span查找器
            root_cause_finder = FindRootCauseSpans(
                client=sls_client,
                project_name=self.sls_config.project_name,
                logstore_name=self.sls_config.logstore_name,
                region=self.sls_config.region,
                start_time=start_time,
                end_time=end_time
            )
            
            # 3. 查找根因spans
            logger.info("开始查找根因spans...")
            root_cause_span_ids = root_cause_finder.find_root_cause_spans()
            
            if not root_cause_span_ids:
                logger.warning("未找到根因span")
                return AnalysisResult(
                    root_causes=[],
                    confidence="low",
                    evidence=False,
                    error_message="No root cause spans found"
                )
            
            logger.info("找到 %d 个根因span", len(root_cause_span_ids))
            
            # 4. 执行模式分析
            pattern_result = self._analyze_error_patterns(
                start_time, end_time, root_cause_span_ids, candidate_root_causes
            )
            
            if pattern_result:
                return AnalysisResult(
                    root_causes=[pattern_result],
                    confidence="high",
                    evidence=True
                )
            
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message="No pattern analysis result found"
            )
            
        except (SLSClientError, CredentialError) as e:
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message=f"Client error: {e}"
            )
        except (ValueError, TypeError, RuntimeError) as e:
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message=f"Analysis error: {e}"
            )
    
    def analyze_latency_root_cause(self, start_time: str, end_time: str, 
                                  candidate_root_causes: List[str]) -> AnalysisResult:
        """
        分析延迟根因
        
        Args:
            start_time: 开始时间 (YYYY-MM-DD HH:MM:SS)
            end_time: 结束时间 (YYYY-MM-DD HH:MM:SS)
            candidate_root_causes: 候选根因列表
            
        Returns:
            AnalysisResult: 分析结果
        """
        try:
            logger.info("--- 开始执行延迟根因分析 ---")
            
            # 1. 创建SLS客户端
            sls_client = sls_client_manager.get_client()
            
            # 2. 计算正常时间段
            anomaly_start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            normal_start = anomaly_start - timedelta(hours=1)
            normal_end = anomaly_start
            normal_start_time = normal_start.strftime("%Y-%m-%d %H:%M:%S")
            normal_end_time = normal_end.strftime("%Y-%m-%d %H:%M:%S")
            
            # 3. 创建延迟分析器
            finder = FindRootCauseSpansRT(
                client=sls_client,
                project_name=self.sls_config.project_name,
                logstore_name=self.sls_config.logstore_name,
                region=self.sls_config.region,
                start_time=start_time,
                end_time=end_time,
                duration_threshold=self.config.duration_threshold,
                limit_num=1000,
                normal_start_time=normal_start_time,
                normal_end_time=normal_end_time,
                minus_average=True,
                only_top1_per_trace=False
            )
            
            # 4. 查找高延迟spans
            logger.info("开始查找高延迟spans...")
            top_spans = finder.find_top_95_percent_spans()
            
            if not top_spans:
                logger.warning("未找到高延迟span")
                return AnalysisResult(
                    root_causes=[],
                    confidence="low",
                    evidence=False,
                    error_message="No high latency spans found"
                )
            
            logger.info("找到 %d 个高延迟span", len(top_spans))
            
            # 5. 执行模式分析
            pattern_result = self._analyze_latency_patterns(
                start_time, end_time, top_spans, candidate_root_causes
            )
            
            if pattern_result:
                return AnalysisResult(
                    root_causes=[pattern_result],
                    confidence="high",
                    evidence=True
                )
            
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message="No pattern analysis result found"
            )
            
        except (SLSClientError, CredentialError) as e:
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message=f"Client error: {e}"
            )
        except (ValueError, TypeError, RuntimeError) as e:
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message=f"Analysis error: {e}"
            )
    
    def _analyze_error_patterns(self, start_time: str, end_time: str, 
                               span_ids: List[str], candidates: List[str]) -> Optional[str]:
        """分析错误模式"""
        if not span_ids:
            return None
        
        try:
            # 构建span查询条件
            span_conditions = " or ".join([f"spanId='{span_id}'" for span_id in span_ids[:2000]])
            
            # 构建模式分析查询
            pattern_query = f"""
* | set session enable_remote_functions=true ;
set session velox_support_row_constructor_enabled=true;
with t0 as (
    select spanName, serviceName,
           JSON_EXTRACT_SCALAR(resources, '$["k8s.pod.ip"]') AS pod_ip,
           JSON_EXTRACT_SCALAR(resources, '$["k8s.node.name"]') AS node_name,
           JSON_EXTRACT_SCALAR(resources, '$["service.version"]') AS service_version,
           if((statusCode = 2 or statusCode = 3), 'true', 'false') as anomaly_label,
           cast(if((statusCode = 2 or statusCode = 3), 1, 0) as double) as error_count
    from log
    where {span_conditions}
),
t1 as (
    select array_agg(spanName) as spanName,
           array_agg(serviceName) as serviceName,
           array_agg(pod_ip) as pod_ip,
           array_agg(node_name) as node_name,
           array_agg(service_version) as service_version,
           array_agg(anomaly_label) as anomaly_label,
           array_agg(error_count) as error_count
    from t0
),
t2 as (
    select row(spanName, serviceName) as table_row
    from t1
),
t3 as (
    select get_patterns(table_row, ARRAY['spanName', 'serviceName']) as ret
    from t2
)
select * from t3
"""
            
            # 执行查询
            start_timestamp = int(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp())
            end_timestamp = int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp())
            
            logs = sls_client_manager.execute_query(
                pattern_query, start_timestamp, end_timestamp, 1000
            )
            
            if not logs:
                return None
            
            # 解析结果
            for log in logs:
                if 'ret' in log:
                    result = self._parse_pattern_result(log['ret'])
                    if result:
                        # 匹配候选根因
                        matched_candidate = self._match_candidates(result, candidates)
                        if matched_candidate:
                            return matched_candidate
            
            return None
            
        except (ValueError, KeyError, TypeError) as e:
            logger.error("错误模式分析失败: %s", e)
            return None
    
    def _analyze_latency_patterns(self, start_time: str, end_time: str, 
                                 span_ids: List[str], candidates: List[str]) -> Optional[str]:
        """分析延迟模式"""
        if not span_ids:
            return None
        
        try:
            # 构建span查询条件
            span_conditions = " or ".join([f"spanId='{span_id}'" for span_id in span_ids[:2000]])
            param_str = '{"minimum_support_fraction": 0.03}'
            
            # 构建差异模式查询
            diff_pattern_query = f"""
duration > {self.config.duration_threshold} | set session enable_remote_functions=true; 
set session velox_support_row_constructor_enabled=true;
with t0 as (
    select spanName, serviceName, cast(duration as double) as duration,
           JSON_EXTRACT_SCALAR(resources, '$["k8s.pod.ip"]') AS pod_ip,
           JSON_EXTRACT_SCALAR(resources, '$["k8s.node.name"]') AS node_name,
           JSON_EXTRACT_SCALAR(resources, '$["service.version"]') AS service_version,
           if(({span_conditions}), 'true', 'false') as anomaly_label,
           cast(if((statusCode = 2 or statusCode = 3), 1, 0) as double) as error_count
    from log
),
t1 as (
    select array_agg(spanName) as spanName,
           array_agg(serviceName) as serviceName,
           array_agg(duration) as duration,
           array_agg(pod_ip) as pod_ip,
           array_agg(node_name) as node_name,
           array_agg(service_version) as service_version,
           array_agg(anomaly_label) as anomaly_label,
           array_agg(error_count) as error_count
    from t0
),
t2 as (
    select row(spanName, serviceName, anomaly_label) as table_row
    from t1
),
t3 as (
    select diff_patterns(table_row, ARRAY['spanName', 'serviceName', 'anomaly_label'], 
                        'anomaly_label', 'true', 'false', '', '', '{param_str}') as ret
    from t2
)
select * from t3
"""
            
            # 执行查询
            start_timestamp = int(datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp())
            end_timestamp = int(datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp())
            
            logs = sls_client_manager.execute_query(
                diff_pattern_query, start_timestamp, end_timestamp, 100
            )
            
            if not logs:
                return None
            
            # 解析结果
            for log in logs:
                if 'ret' in log:
                    result = self._parse_pattern_result(log['ret'])
                    if result:
                        # 匹配候选根因
                        matched_candidate = self._match_candidates(result, candidates)
                        if matched_candidate:
                            return matched_candidate
            
            return None
            
        except (ValueError, KeyError, TypeError) as e:
            logger.error("延迟模式分析失败: %s", e)
            return None
    
    def _parse_pattern_result(self, result_str: str) -> Optional[Dict[str, Any]]:
        """解析模式分析结果"""
        try:
            import ast
            if isinstance(result_str, str):
                data_str = result_str.replace('null', 'None')
                result = ast.literal_eval(data_str)
            else:
                result = result_str
            
            if len(result) >= 2 and isinstance(result[0], list) and isinstance(result[1], list):
                return {
                    'patterns': result[0],
                    'counts': result[1]
                }
            
            return None
            
        except (ValueError, SyntaxError) as e:
            logger.error("解析模式结果失败: %s", e)
            return None
    
    def _match_candidates(self, pattern_result: Dict[str, Any], 
                         candidates: List[str]) -> Optional[str]:
        """匹配候选根因"""
        patterns = pattern_result.get('patterns', [])
        counts = pattern_result.get('counts', [])
        
        # 提取服务名称
        service_patterns = {}
        for i, pattern in enumerate(patterns):
            if i < len(counts) and 'serviceName' in pattern and '=' in pattern:
                try:
                    service = pattern.split("serviceName=")[1].strip('"\'')
                    service_patterns[service] = service_patterns.get(service, 0) + counts[i]
                except (ValueError, IndexError):
                    continue
        
        if not service_patterns:
            return None
        
        # 按频率排序
        sorted_services = sorted(service_patterns.items(), key=lambda x: x[1], reverse=True)
        
        # 匹配候选根因
        for service_name, _ in sorted_services:
            possible_candidates = [
                f"{service_name}.cpu",
                f"{service_name}.memory", 
                f"{service_name}.networkLatency",
                f"{service_name}.latency",
                f"{service_name}.Failure",
                f"{service_name}.LargeGc",
                f"{service_name}.Unreachable",
                f"{service_name}.CacheFailure",
                f"{service_name}.FloodHomepage",
                service_name
            ]
            
            for candidate in possible_candidates:
                if candidate in candidates:
                    logger.info("✅ 模式匹配成功: %s", candidate)
                    return candidate
        
        return None


# 全局分析器实例
root_cause_analyzer = RootCauseAnalyzer()
