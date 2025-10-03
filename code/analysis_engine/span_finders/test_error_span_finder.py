import os
import sys
import pytest
import logging
from dotenv import load_dotenv


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
    logger.info(f"已从 {env_path} 加载环境变量")
else:
    logger.warning(f"未找到 .env 文件: {env_path}")


def test_root_cause_spans_query():
    """测试 root_cause_spans_query 方法：连接真实客户端，查询结果并输出"""
    from analysis_engine.span_finders.error_span_finder import FindRootCauseSpans
    from aliyun.log import LogClient
    from aliyun.log.logexception import LogException

    # 检查环境变量是否存在
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    logger.info("🔍 校验环境变量获取...")
    logger.info(
        f"  ALIBABA_CLOUD_ACCESS_KEY_ID: {access_key_id} {'已设置' if access_key_id else '未设置'}"
    )
    logger.info(
        f"  ALIBABA_CLOUD_ACCESS_KEY_SECRET: {'已设置' if access_key_secret else '未设置'}"
    )

    if not access_key_id or not access_key_secret:
        logger.error("❌ 环境变量校验失败")
        raise ValueError(
            "需要设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET"
        )

    logger.info("✅ 环境变量校验通过")

    logger.info("🚀 开始测试 root_cause_spans_query 方法")

    # 使用指定的参数
    project_name = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
    logstore_name = "logstore-tracing"
    region = "cn-qingdao"
    start_time = "2025-08-28 15:08:03"
    end_time = "2025-08-28 15:13:03"

    try:
        # 创建真实的客户端
        logger.info("📡 创建阿里云SLS客户端...")
        client = sls_client_manager.get_client()

        # 创建 FindRootCauseSpans 实例
        logger.info("🔧 创建 FindRootCauseSpans 实例...")
        find_root_cause_spans = FindRootCauseSpans(
            client=client,
            project_name=project_name,
            logstore_name=logstore_name,
            region=region,
            start_time=start_time,
            end_time=end_time,
        )

        # 执行查询
        logger.info("🔍 开始执行 root_cause_spans_query() 方法...")
        query = find_root_cause_spans.root_cause_spans_query()

        # 输出结果
        logger.info("📊 查询结果:")
        logger.info(f"  项目名称: {project_name}")
        logger.info(f"  Logstore: {logstore_name}")
        logger.info(f"  区域: {region}")
        logger.info(f"  时间范围: {start_time} ~ {end_time}")
        logger.info(f"  生成的查询语句: {query}")

        # 验证返回结果
        assert isinstance(query, str), (
            f"查询语句应该是字符串类型，实际类型: {type(query)}"
        )

        logger.info("🎉 测试完成")

    except LogException as e:
        # 处理权限或其他阿里云API错误
        logger.error(f"❌ 阿里云API错误: {e}")
        if "Unauthorized" in str(e):
            logger.error(f"权限不足，无法访问阿里云SLS: {e}")
            raise PermissionError(f"权限不足，无法访问阿里云SLS: {e}")
        else:
            # 其他类型的错误，重新抛出
            raise
    except Exception as e:
        # 其他类型的错误
        logger.error(f"❌ 测试过程中发生错误: {e}")
        raise


if __name__ == "__main__":
    # 直接运行测试
    test_root_cause_spans_query()
