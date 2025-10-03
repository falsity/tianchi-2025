import os
import sys
import pytest
import logging
from dotenv import load_dotenv
from datetime import datetime

# 设置正确的Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, "..", "..", "..")
code_dir = os.path.join(project_root, "code")
sys.path.insert(0, project_root)
sys.path.insert(0, code_dir)
from sls_client_manager import sls_client_manager

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)
logger = logging.getLogger(__name__)

# 加载.env文件中的环境变量
env_path = os.path.join(project_root, ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)
    logger.info("已从 %s 加载环境变量", env_path)
else:
    logger.warning("未找到 .env 文件: %s", env_path)


def test_find_top_95_percent_spans():
    """测试 find_top_95_percent_spans 方法：连接真实客户端，查询结果并输出"""
    from analysis_engine.span_finders.latency_span_finder import FindRootCauseSpansRT
    from aliyun.log import LogClient
    from aliyun.log.logexception import LogException

    # 检查环境变量是否存在
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    logger.info("🔍 校验环境变量获取...")
    logger.info(
        "  ALIBABA_CLOUD_ACCESS_KEY_ID: %s",
        "已设置" if access_key_id else "未设置",
    )
    logger.info(
        "  ALIBABA_CLOUD_ACCESS_KEY_SECRET: %s",
        "已设置" if access_key_secret else "未设置",
    )

    if not access_key_id or not access_key_secret:
        logger.error("❌ 环境变量校验失败")
        raise ValueError(
            "需要设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET"
        )

    logger.info("✅ 环境变量校验通过")

    logger.info("🚀 开始测试 find_top_95_percent_spans 方法")

    # 使用指定的参数
    project_name = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
    logstore_name = "logstore-tracing"
    region = "cn-qingdao"
    start_time = "2025-08-28 15:08:03"
    end_time = "2025-08-28 15:13:03"
    duration_threshold = 1000000  # 1秒
    limit_num = 1000
    normal_start_time = "2025-08-28 14:58:03"
    normal_end_time = "2025-08-28 15:03:03"

    try:
        # 创建真实的客户端
        logger.info("📡 创建阿里云SLS客户端...")
        client = sls_client_manager.get_client()

        # 创建 FindRootCauseSpansRT 实例
        logger.info("🔧 创建 FindRootCauseSpansRT 实例...")
        find_root_cause_spans_rt = FindRootCauseSpansRT(
            client=client,
            project_name=project_name,
            logstore_name=logstore_name,
            region=region,
            start_time=start_time,
            end_time=end_time,
            duration_threshold=duration_threshold,
            limit_num=limit_num,
            normal_start_time=normal_start_time,
            normal_end_time=normal_end_time,
            minus_average=True,  # 启用减去平均值的功能
            only_top1_per_trace=False,  # 处理所有span
        )

        # 执行查询
        logger.info("🔍 开始执行 find_top_95_percent_spans() 方法...")
        top_spans = find_root_cause_spans_rt.find_top_95_percent_spans()

        # 输出结果
        logger.info("📊 查询结果:")
        logger.info("  项目名称: %s", project_name)
        logger.info("  Logstore: %s", logstore_name)
        logger.info("  区域: %s", region)
        logger.info("  异常时间范围: %s ~ %s", start_time, end_time)
        logger.info("  正常时间范围: %s ~ %s", normal_start_time, normal_end_time)
        logger.info("  延迟阈值: %s 微秒", duration_threshold)
        logger.info("  限制数量: %s", limit_num)
        logger.info("  找到的前95%%独占时间span数量: %s", len(top_spans))

        # 验证返回结果
        assert isinstance(top_spans, list), (
            f"返回结果应该是列表类型，实际类型: {type(top_spans)}"
        )

        # 结果分析
        if top_spans:
            logger.info("✅ 找到前95%独占时间的span")
            logger.info(
                "📋 前95%%独占时间的span_id列表: %s%s",
                top_spans[:10],
                "..." if len(top_spans) > 10 else "",
            )

            # 测试获取查询语句
            logger.info("🔍 测试获取查询语句...")
            span_conditions, query = (
                find_root_cause_spans_rt.get_top_95_percent_spans_query()
            )
            logger.info(
                "📋 查询条件: %s%s",
                span_conditions[:100],
                "..." if len(span_conditions) > 100 else "",
            )
            logger.info(
                "📋 完整查询语句: %s%s", query[:200], "..." if len(query) > 200 else ""
            )

            # 验证查询语句格式
            assert isinstance(query, str), (
                f"查询语句应该是字符串类型，实际类型: {type(query)}"
            )
            assert "select" in query.lower(), "查询语句应该包含select关键字"
            assert "spanid" in query.lower(), "查询语句应该包含spanid字段"
        else:
            logger.info("ℹ️ 未找到前95%独占时间的span")

        logger.info("🎉 测试完成")

    except LogException as e:
        # 处理权限或其他阿里云API错误
        logger.error("❌ 阿里云API错误: %s", e)
        if "Unauthorized" in str(e):
            logger.error("权限不足，无法访问阿里云SLS: %s", e)
            raise PermissionError(f"权限不足，无法访问阿里云SLS: {e}")
        else:
            # 其他类型的错误，重新抛出
            raise
    except Exception as e:
        # 其他类型的错误
        logger.error("❌ 测试过程中发生错误: %s", e)
        raise


def test_find_top_95_percent_spans_without_minus_average():
    """测试不减去平均值的情况"""
    from analysis_engine.span_finders.latency_span_finder import FindRootCauseSpansRT
    from aliyun.log.logexception import LogException

    # 检查环境变量是否存在
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    if not access_key_id or not access_key_secret:
        logger.error("❌ 环境变量校验失败")
        raise ValueError(
            "需要设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET"
        )

    logger.info("🚀 开始测试不减去平均值的情况")

    # 使用指定的参数
    project_name = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
    logstore_name = "logstore-tracing"
    region = "cn-qingdao"
    start_time = "2025-08-28 15:08:03"
    end_time = "2025-08-28 15:13:03"
    duration_threshold = 1000000  # 1秒
    limit_num = 1000

    try:
        # 创建真实的客户端
        client = sls_client_manager.get_client()

        # 创建 FindRootCauseSpansRT 实例（不减去平均值）
        find_root_cause_spans_rt = FindRootCauseSpansRT(
            client=client,
            project_name=project_name,
            logstore_name=logstore_name,
            region=region,
            start_time=start_time,
            end_time=end_time,
            duration_threshold=duration_threshold,
            limit_num=limit_num,
            minus_average=False,  # 不减去平均值
            only_top1_per_trace=False,
        )

        # 执行查询
        top_spans = find_root_cause_spans_rt.find_top_95_percent_spans()

        # 输出结果
        logger.info("📊 不减去平均值的查询结果:")
        logger.info("  找到的前95%%独占时间span数量: %s", len(top_spans))

        # 验证返回结果
        assert isinstance(top_spans, list), (
            f"返回结果应该是列表类型，实际类型: {type(top_spans)}"
        )

        logger.info("✅ 不减去平均值的测试完成")

    except LogException as e:
        if "Unauthorized" in str(e):
            raise PermissionError(f"权限不足，无法访问阿里云SLS: {e}")
        else:
            raise
    except Exception as e:
        logger.error("❌ 测试过程中发生错误: %s", e)
        raise


def test_only_top1_per_trace():
    """测试只处理每个trace中独占时间排top-1的span"""
    from analysis_engine.span_finders.latency_span_finder import FindRootCauseSpansRT
    from aliyun.log.logexception import LogException

    # 检查环境变量是否存在
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    if not access_key_id or not access_key_secret:
        logger.error("❌ 环境变量校验失败")
        raise ValueError(
            "需要设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET"
        )

    logger.info("🚀 开始测试只处理每个trace中top-1的span")

    # 使用指定的参数
    project_name = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
    logstore_name = "logstore-tracing"
    region = "cn-qingdao"
    start_time = "2025-08-28 15:08:03"
    end_time = "2025-08-28 15:13:03"
    duration_threshold = 1000000  # 1秒
    limit_num = 1000

    try:
        # 创建真实的客户端
        client = sls_client_manager.get_client()

        # 测试处理所有span的情况
        logger.info("📊 测试处理所有span的情况:")
        find_root_cause_spans_rt_all = FindRootCauseSpansRT(
            client=client,
            project_name=project_name,
            logstore_name=logstore_name,
            region=region,
            start_time=start_time,
            end_time=end_time,
            duration_threshold=duration_threshold,
            limit_num=limit_num,
            only_top1_per_trace=False,  # 处理所有span
        )

        top_spans_all = find_root_cause_spans_rt_all.find_top_95_percent_spans()
        logger.info("  处理所有span时找到的前95%%span数量: %s", len(top_spans_all))

        # 测试只处理top-1的情况
        logger.info("📊 测试只处理每个trace中top-1的span:")
        find_root_cause_spans_rt_top1 = FindRootCauseSpansRT(
            client=client,
            project_name=project_name,
            logstore_name=logstore_name,
            region=region,
            start_time=start_time,
            end_time=end_time,
            duration_threshold=duration_threshold,
            limit_num=limit_num,
            only_top1_per_trace=True,  # 只处理每个trace中top-1的span
        )

        top_spans_top1 = find_root_cause_spans_rt_top1.find_top_95_percent_spans()
        logger.info("  只处理top-1 span时找到的前95%%span数量: %s", len(top_spans_top1))

        # 对比结果
        logger.info("📊 对比结果:")
        logger.info("  处理所有span: %s 个span", len(top_spans_all))
        logger.info("  只处理top-1: %s 个span", len(top_spans_top1))

        if len(top_spans_all) > 0:
            reduction_rate = (
                (len(top_spans_all) - len(top_spans_top1)) / len(top_spans_all) * 100
            )
            logger.info("  计算量减少: %.1f%%", reduction_rate)

        # 验证返回结果
        assert isinstance(top_spans_all, list), (
            f"处理所有span的返回结果应该是列表类型，实际类型: {type(top_spans_all)}"
        )
        assert isinstance(top_spans_top1, list), (
            f"只处理top-1的返回结果应该是列表类型，实际类型: {type(top_spans_top1)}"
        )

        logger.info("✅ only_top1_per_trace 测试完成")

    except LogException as e:
        if "Unauthorized" in str(e):
            raise PermissionError(f"权限不足，无法访问阿里云SLS: {e}")
        else:
            raise
    except Exception as e:
        logger.error("❌ 测试过程中发生错误: %s", e)
        raise


def test_top1_with_minus_average_logic():
    """测试 only_top1_per_trace=True 且 minus_average=True 时的逻辑正确性"""
    from analysis_engine.span_finders.latency_span_finder import FindRootCauseSpansRT
    from aliyun.log.logexception import LogException

    # 检查环境变量是否存在
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    if not access_key_id or not access_key_secret:
        logger.error("❌ 环境变量校验失败")
        raise ValueError(
            "需要设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET"
        )

    logger.info("🚀 开始测试 only_top1_per_trace + minus_average 的逻辑正确性")

    # 使用指定的参数
    project_name = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
    logstore_name = "logstore-tracing"
    region = "cn-qingdao"
    start_time = "2025-08-28 15:08:03"
    end_time = "2025-08-28 15:13:03"
    duration_threshold = 1000000  # 1秒
    limit_num = 1000
    normal_start_time = "2025-08-28 14:58:03"
    normal_end_time = "2025-08-28 15:03:03"

    try:
        # 创建真实的客户端
        client = sls_client_manager.get_client()

        # 创建 FindRootCauseSpansRT 实例（启用减去平均值且只处理top-1）
        find_root_cause_spans_rt = FindRootCauseSpansRT(
            client=client,
            project_name=project_name,
            logstore_name=logstore_name,
            region=region,
            start_time=start_time,
            end_time=end_time,
            duration_threshold=duration_threshold,
            limit_num=limit_num,
            normal_start_time=normal_start_time,
            normal_end_time=normal_end_time,
            minus_average=True,  # 启用减去平均值
            only_top1_per_trace=True,  # 只处理每个trace的top-1
        )

        # 执行查询
        top_spans = find_root_cause_spans_rt.find_top_95_percent_spans()

        # 输出结果
        logger.info("📊 only_top1_per_trace + minus_average 的查询结果:")
        logger.info("  找到的span数量: %s", len(top_spans))
        logger.info("✅ 这些span是基于调整后独占时间（减去平均值后）选择的top-1")

        # 验证返回结果
        assert isinstance(top_spans, list), (
            f"返回结果应该是列表类型，实际类型: {type(top_spans)}"
        )

        logger.info("✅ only_top1_per_trace + minus_average 逻辑测试完成")

    except LogException as e:
        if "Unauthorized" in str(e):
            raise PermissionError(f"权限不足，无法访问阿里云SLS: {e}")
        else:
            raise
    except Exception as e:
        logger.error("❌ 测试过程中发生错误: %s", e)
        raise


if __name__ == "__main__":
    # 直接运行测试
    test_find_top_95_percent_spans()
    # test_find_top_95_percent_spans_without_minus_average()
    # test_only_top1_per_trace()
    # test_top1_with_minus_average_logic()
