"""
延迟分析器模块

专门负责延迟根因分析的逻辑，包括高延迟span识别和延迟模式分析。
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from config import config_manager
from sls_client_manager import sls_client_manager, SLSClientError
from credential_manager import CredentialError
from find_root_cause_spans_rt import FindRootCauseSpansRT
from .shared_models import AnalysisResult

# 设置日志记录器
logger = logging.getLogger(__name__)


class LatencyAnalyzer:
    """延迟分析器"""

    def __init__(self):
        self.config = config_manager.get_analysis_config()
        self.sls_config = config_manager.get_sls_config()

    def analyze_latency_root_cause(
        self, start_time: str, end_time: str, candidate_root_causes: List[str]
    ) -> AnalysisResult:
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
                only_top1_per_trace=False,
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
                    error_message="No high latency spans found",
                )

            logger.info("找到 %d 个高延迟span", len(top_spans))

            # 5. 执行模式分析
            pattern_result = self._analyze_latency_patterns(
                start_time, end_time, top_spans, candidate_root_causes
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

    def _analyze_latency_patterns(
        self, start_time: str, end_time: str, span_ids: List[str], candidates: List[str]
    ) -> Optional[str]:
        """分析延迟模式 - 与STS_Root_Cause_Analysis_Latency.py保持一致"""
        if not span_ids:
            return None

        try:
            # 构建span查询条件
            span_conditions = " or ".join(
                [f"spanId='{span_id}'" for span_id in span_ids[:2000]]
            )
            param_str = '{"minimum_support_fraction": 0.03}'

            # 构建差异模式查询 - 延迟分析使用duration条件
            diff_pattern_query = f"""
duration > {self.config.duration_threshold} | set session enable_remote_functions=true; set session velox_support_row_constructor_enabled=true;
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
            start_timestamp = int(
                datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp()
            )
            end_timestamp = int(
                datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S").timestamp()
            )

            logs = sls_client_manager.execute_query(
                diff_pattern_query, start_timestamp, end_timestamp, 100
            )

            if not logs:
                return None

            # 解析结果 - 与STS_Root_Cause_Analysis_Latency.py逻辑一致
            service_patterns = {}
            span_patterns = []

            for log_entry in logs:
                if "ret" in log_entry:
                    try:
                        data_str = log_entry["ret"].replace("null", "None")
                        result = eval(data_str)
                        if (
                            len(result) >= 2
                            and isinstance(result[0], list)
                            and isinstance(result[1], list)
                        ):
                            patterns, counts = result[0], result[1]
                            for i, pattern in enumerate(patterns):
                                if i < len(counts):
                                    count = counts[i]
                                    if "serviceName" in pattern and "=" in pattern:
                                        import re

                                        match = re.search(
                                            r'"serviceName"=\'([^\']+)\'', pattern
                                        )
                                        if match:
                                            service_patterns[match.group(1)] = (
                                                service_patterns.get(match.group(1), 0)
                                                + count
                                            )
                                    elif "spanName" in pattern:
                                        span_name = (
                                            pattern.split("=")[1].strip("'\"")
                                            if "=" in pattern
                                            else pattern
                                        )
                                        span_patterns.append((span_name, count))
                    except Exception as e:
                        logger.error("⚠️ 解析模式结果失败: %s", e)
                        continue

            # 如果没有服务模式，尝试从span模式推断服务
            if not service_patterns and span_patterns:
                service_candidates = self._map_span_to_service(span_patterns)
                if service_candidates:
                    service_patterns = service_candidates

            # 额外的span模式映射
            for span_name, count in span_patterns:
                if (
                    "RecommendationService" in span_name
                    or "get_product_list" in span_name
                ):
                    service_patterns["recommendation"] = (
                        service_patterns.get("recommendation", 0) + count
                    )
                elif "CheckoutService" in span_name:
                    service_patterns["checkout"] = (
                        service_patterns.get("checkout", 0) + count
                    )
                elif "Currency/" in span_name or "CurrencyService" in span_name:
                    service_patterns["currency"] = (
                        service_patterns.get("currency", 0) + count
                    )
                elif "CartService" in span_name:
                    service_patterns["cart"] = service_patterns.get("cart", 0) + count
                elif "router flagservice egress" in span_name:
                    service_patterns["ad"] = service_patterns.get("ad", 0) + count

            # 匹配候选根因
            if service_patterns:
                all_service_matches = [
                    (service, count) for service, count in service_patterns.items()
                ]
                all_service_matches.sort(key=lambda x: x[1], reverse=True)
                logger.info(
                    "🔍 模式分析识别的服务: %s", [s[0] for s in all_service_matches]
                )

                for service_name, pattern_count in all_service_matches:
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
                        service_name,
                    ]
                    for candidate in possible_candidates:
                        if candidate in candidates:
                            logger.info("✅ 模式匹配成功: %s", candidate)
                            return candidate
                logger.info("⚠️ 模式分析的服务未匹配到任何候选根因")
            else:
                logger.info("⚠️ 未识别到服务模式，尝试基于候选根因进行匹配")

            # 基于候选根因进行匹配
            for candidate in candidates:
                if "." in candidate and (
                    candidate.endswith(".cpu") or candidate.endswith(".memory")
                ):
                    service_name = candidate.split(".")[0]
                    possible_candidates = [
                        f"{service_name}.cpu",
                        f"{service_name}.memory",
                        service_name,
                    ]
                    for possible in possible_candidates:
                        if possible in candidates:
                            logger.info("✅ 基于候选根因匹配成功: %s", possible)
                            return possible

            logger.info("❌ 未找到匹配的根因")
            return None

        except (ValueError, KeyError, TypeError) as e:
            logger.error("延迟模式分析失败: %s", e)
            return None

    def _map_span_to_service(self, span_patterns: List[tuple]) -> dict:
        """将span模式映射到服务 - 与STS_Root_Cause_Analysis_Latency.py逻辑一致"""
        service_candidates = {}

        for span_name, count in span_patterns:
            if "CartService" in span_name or "cart" in span_name.lower():
                service_candidates["cart"] = service_candidates.get("cart", 0) + count
            elif (
                "ProductCatalogService" in span_name
                or "product-catalog" in span_name.lower()
                or "product" in span_name.lower()
            ):
                service_candidates["product-catalog"] = (
                    service_candidates.get("product-catalog", 0) + count
                )
            elif "PaymentService" in span_name or "payment" in span_name.lower():
                service_candidates["payment"] = (
                    service_candidates.get("payment", 0) + count
                )
            elif "CheckoutService" in span_name or "checkout" in span_name.lower():
                service_candidates["checkout"] = (
                    service_candidates.get("checkout", 0) + count
                )
            elif (
                "RecommendationService" in span_name
                or "recommendation" in span_name.lower()
                or "get_product_list" in span_name
            ):
                service_candidates["recommendation"] = (
                    service_candidates.get("recommendation", 0) + count
                )
            elif (
                "CurrencyService" in span_name
                or "currency" in span_name.lower()
                or "Currency/" in span_name
            ):
                service_candidates["currency"] = (
                    service_candidates.get("currency", 0) + count
                )
            elif (
                "flagservice" in span_name.lower()
                or "router flagservice egress" in span_name
            ):
                service_candidates["ad"] = service_candidates.get("ad", 0) + count
            elif "InventoryService" in span_name or "inventory" in span_name.lower():
                service_candidates["inventory"] = (
                    service_candidates.get("inventory", 0) + count
                )
            elif (
                "ImageProviderService" in span_name
                or "image-provider" in span_name.lower()
            ):
                service_candidates["image-provider"] = (
                    service_candidates.get("image-provider", 0) + count
                )
            elif "frontend" in span_name.lower() and "proxy" not in span_name.lower():
                service_candidates["frontend"] = (
                    service_candidates.get("frontend", 0) + count
                )
            elif "load-generator" in span_name.lower():
                service_candidates["load-generator"] = (
                    service_candidates.get("load-generator", 0) + count
                )

        return service_candidates


# 全局延迟分析器实例
latency_analyzer = LatencyAnalyzer()
