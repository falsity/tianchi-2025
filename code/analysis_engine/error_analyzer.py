"""
错误分析器模块

专门负责错误根因分析的逻辑，包括错误模式识别和根因定位。
"""

import logging
from datetime import datetime
from typing import List, Optional

from config import config_manager
from sls_client_manager import sls_client_manager, SLSClientError
from credential_manager import CredentialError
from .span_finders.error_span_finder import FindRootCauseSpans
from .shared_models import AnalysisResult

# 设置日志记录器
logger = logging.getLogger(__name__)


class ErrorAnalyzer:
    """错误分析器"""

    def __init__(self):
        self.config = config_manager.get_analysis_config()
        self.sls_config = config_manager.get_sls_config()

    def analyze_error_root_cause(
        self, start_time: str, end_time: str, candidate_root_causes: List[str]
    ) -> AnalysisResult:
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
                end_time=end_time,
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
                    error_message="No root cause spans found",
                )

            logger.info("找到 %d 个根因span", len(root_cause_span_ids))

            # 4. 执行模式分析
            pattern_result = self._analyze_error_patterns(
                start_time, end_time, root_cause_span_ids, candidate_root_causes
            )

            if pattern_result:
                return AnalysisResult(
                    root_causes=[pattern_result], confidence="high", evidence=True
                )

            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message="No pattern analysis result found",
            )

        except (SLSClientError, CredentialError) as e:
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message=f"Client error: {e}",
            )
        except (ValueError, TypeError, RuntimeError) as e:
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message=f"Analysis error: {e}",
            )

    def _analyze_error_patterns(
        self, start_time: str, end_time: str, span_ids: List[str], candidates: List[str]
    ) -> Optional[str]:
        """分析错误模式 - 与STS_Root_Cause_Analysis_Error.py保持一致"""
        if not span_ids:
            return None

        try:
            # 构建span查询条件
            span_conditions = " or ".join(
                [f"spanId='{span_id}'" for span_id in span_ids[:2000]]
            )

            # 1. 执行get_patterns查询
            get_patterns_result = self._execute_get_patterns_query(
                start_time, end_time, span_conditions
            )

            # 2. 执行diff_patterns查询
            diff_patterns_result = self._execute_diff_patterns_query(
                start_time, end_time, span_conditions
            )

            # 3. 解析服务证据（与STS_Root_Cause_Analysis_Error.py逻辑一致）
            target_service = self._parse_service_from_evidence(
                get_patterns_result, diff_patterns_result, candidates
            )

            if target_service and target_service != "unknown":
                return f"{target_service}.Failure"

            return None

        except (ValueError, KeyError, TypeError) as e:
            logger.error("错误模式分析失败: %s", e)
            return None

    def _execute_get_patterns_query(
        self, start_time: str, end_time: str, span_conditions: str
    ) -> Optional[any]:
        """执行get_patterns查询"""
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

        start_timestamp = int(
            datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp()
        )
        end_timestamp = int(
            datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp()
        )

        logs = sls_client_manager.execute_query(
            pattern_query, start_timestamp, end_timestamp, 1000
        )

        if not logs:
            return None

        for log in logs:
            if "ret" in log:
                return log["ret"]

        return None

    def _execute_diff_patterns_query(
        self, start_time: str, end_time: str, span_conditions: str
    ) -> Optional[any]:
        """执行diff_patterns查询"""
        param_str = '{"minimum_support_fraction": 0.03}'
        diff_pattern_query = f"""
statusCode>0 | set session enable_remote_functions=true ;
set session velox_support_row_constructor_enabled=true;
with t0 as (
    select spanName, serviceName,
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
           array_agg(pod_ip) as pod_ip,
           array_agg(node_name) as node_name,
           array_agg(service_version) as service_version,
           array_agg(anomaly_label) as anomaly_label,
           array_agg(error_count) as error_count
    from t0
),
t2 as (
    select row(serviceName, anomaly_label) as table_row
    from t1
),
t3 as (
    select diff_patterns(table_row, ARRAY['serviceName', 'anomaly_label'], 'anomaly_label', 'true', 'false', '', '', '{param_str}') as ret
    from t2
)
select * from t3
"""

        start_timestamp = int(
            datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp()
        )
        end_timestamp = int(
            datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp()
        )

        logs = sls_client_manager.execute_query(
            diff_pattern_query, start_timestamp, end_timestamp, 1000
        )

        if not logs:
            return None

        for log in logs:
            if "ret" in log:
                return log["ret"]

        return None

    def _parse_service_from_evidence(
        self,
        get_patterns_result: any,
        diff_patterns_result: any,
        candidate_root_causes: List[str],
    ) -> str:
        """解析服务证据 - 与STS_Root_Cause_Analysis_Error.py逻辑一致"""
        target_service = "unknown"

        # 提取候选服务（只考虑.Failure类型）
        candidate_services = set()
        if candidate_root_causes:
            for candidate in candidate_root_causes:
                if "." in candidate and candidate.endswith(".Failure"):
                    service_name = candidate.split(".")[0]
                    candidate_services.add(service_name)

        logger.info("🎯 限制分析到候选服务: %s", list(candidate_services))

        # 解析get_patterns结果 - 只考虑候选服务
        if get_patterns_result:
            try:
                if isinstance(get_patterns_result, str):
                    data_str = get_patterns_result.replace("null", "None")
                    result = eval(data_str)
                else:
                    result = get_patterns_result

                if (
                    len(result) >= 2
                    and isinstance(result[0], list)
                    and isinstance(result[1], list)
                ):
                    service_patterns = result[0]
                    service_counts = result[1]

                    # 只查找候选服务 - 忽略其他服务
                    candidate_matches = []

                    for i, pattern in enumerate(service_patterns):
                        if i < len(service_counts):
                            count = service_counts[i]
                            if "serviceName=" in pattern:
                                service = pattern.split("serviceName=")[1].strip("\"'")

                                # 只考虑候选服务
                                if service in candidate_services:
                                    candidate_matches.append((service, count))
                                    logger.info(
                                        "🎯 找到候选服务: %s (错误数: %d)",
                                        service,
                                        count,
                                    )
                                else:
                                    logger.info("❌ 忽略非候选服务: %s", service)

                    # 使用错误数最高的候选服务
                    if candidate_matches:
                        best_candidate = max(candidate_matches, key=lambda x: x[1])
                        target_service = best_candidate[0]
                        logger.info(
                            "✅ 选择候选服务: %s (错误数: %d)",
                            target_service,
                            best_candidate[1],
                        )
                    else:
                        logger.info("❌ 在错误模式中未找到候选服务")
                        return "unknown"

            except Exception as e:
                logger.error("⚠️ get_patterns结果解析失败: %s", e)

        # 解析diff_patterns结果
        if diff_patterns_result:
            try:
                if isinstance(diff_patterns_result, str):
                    data_str = diff_patterns_result.replace("null", "None")
                    result = eval(data_str)
                else:
                    result = diff_patterns_result

                if len(result) >= 1 and isinstance(result[0], list):
                    anomaly_patterns = result[0]

                    for pattern in anomaly_patterns:
                        if "serviceName" in pattern and "=" in pattern:
                            service = pattern.split("='")[1].strip("'\"")
                            logger.info("✅ diff_patterns确认异常服务: %s", service)

                            if service == target_service:
                                logger.info(
                                    "✅ 多重证据确认: %s 是主要根因服务", service
                                )
                                return target_service
                            elif target_service == "unknown":
                                target_service = service
                                return target_service

            except Exception as e:
                logger.error("⚠️ diff_patterns结果解析失败: %s", e)

        if target_service != "unknown":
            return target_service
        else:
            logger.info("❌ 无法从运行时证据中提取明确的目标服务")
            return "unknown"


# 全局错误分析器实例
error_analyzer = ErrorAnalyzer()
