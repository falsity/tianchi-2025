"""
重构后的主程序

使用模块化架构，提供清晰、可维护的根因分析功能。
"""

import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

from config import AnalysisType, config_manager
from sls_client_manager import sls_client_manager
from analysis_engine import root_cause_analyzer, AnalysisResult


def load_env_variables() -> bool:
    """从 .env 文件加载环境变量"""
    logger = logging.getLogger(__name__)
    try:
        from dotenv import load_dotenv
        
        # 尝试多个路径
        env_paths = [
            ".env",  # 当前目录
            "../.env",  # 父目录
            os.path.join(os.path.dirname(__file__), "..", ".env")  # 相对于脚本的父目录
        ]
        
        loaded = False
        for env_path in env_paths:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                logger.info("✅ 已从 %s 文件加载环境变量", env_path)
                loaded = True
                break
        
        if not loaded:
            # 如果都没找到，尝试默认加载
            load_dotenv()
            logger.info("✅ 已从默认位置加载环境变量")
        
        return True
    except ImportError:
        logger.error("❌ python-dotenv 未安装，请运行: pip install python-dotenv")
        return False
    except (FileNotFoundError, OSError, ValueError) as e:
        logger.error("❌ 加载 .env 文件失败: %s", e)
        return False


def setup_logger() -> logging.Logger:
    """设置日志记录器"""
    # 配置根日志记录器，这样所有子模块都会继承这个配置
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 清除现有处理器避免重复
    root_logger.handlers.clear()
    
    # 文件处理器
    file_handler = logging.FileHandler('analysis.log', mode='w', encoding='utf-8')
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # 返回主模块的日志记录器
    return logging.getLogger(__name__)


class Main:
    """重构后的主程序类"""
    
    def __init__(self, file_name: str):
        """初始化主程序"""
        self.file_name = file_name
        self.logger = setup_logger()
        self.config = config_manager
    
    def read_input_data(self, file_name: str) -> List[Dict[str, Any]]:
        """
        读取输入数据
        
        Args:
            file_name: 输入文件路径
            
        Returns:
            List[Dict[str, Any]]: 解析后的数据列表
        """
        data = []
        
        try:
            with open(file_name, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            item = json.loads(line)
                            data.append(item)
                        except json.JSONDecodeError as e:
                            self.logger.warning(
                                "Failed to parse line: %s... Error: %s", line[:100], e
                            )
                            continue
            
            self.logger.info("Successfully read %d records from %s", len(data), file_name)
            return data
            
        except FileNotFoundError:
            self.logger.error("Input file not found: %s", file_name)
            return []
        except (IOError, OSError) as e:
            self.logger.error("Failed to read input file: %s", e)
            return []
    
    def determine_analysis_type(self, alarm_rules: List[str]) -> AnalysisType:
        """
        确定分析类型
        
        Args:
            alarm_rules: 告警规则列表
            
        Returns:
            AnalysisType: 分析类型
        """
        if not alarm_rules:
            return AnalysisType.ERROR
        
        # 转换为小写进行不区分大小写的匹配
        alarm_rules_lower = [rule.lower() for rule in alarm_rules]
        
        # 检查错误相关规则
        error_indicators = ["error", "failure", "exception", "status"]
        for rule in alarm_rules_lower:
            if any(indicator in rule for indicator in error_indicators):
                return AnalysisType.ERROR
        
        # 检查延迟相关规则
        latency_indicators = ["rt", "latency", "response", "duration", "time"]
        for rule in alarm_rules_lower:
            if any(indicator in rule for indicator in latency_indicators):
                return AnalysisType.LATENCY
        
        # 默认为错误分析
        return AnalysisType.ERROR
    
    def run_analysis(self, analysis_type: AnalysisType, start_time: str, 
                    end_time: str, candidate_root_causes: List[str]) -> AnalysisResult:
        """
        运行分析
        
        Args:
            analysis_type: 分析类型
            start_time: 开始时间
            end_time: 结束时间
            candidate_root_causes: 候选根因列表
            
        Returns:
            AnalysisResult: 分析结果
        """
        try:
            if analysis_type == AnalysisType.ERROR:
                self.logger.info("Starting error analysis for time range: %s to %s", start_time, end_time)
                result = root_cause_analyzer.analyze_error_root_cause(
                    start_time, end_time, candidate_root_causes
                )
            elif analysis_type == AnalysisType.LATENCY:
                self.logger.info("Starting latency analysis for time range: %s to %s", start_time, end_time)
                result = root_cause_analyzer.analyze_latency_root_cause(
                    start_time, end_time, candidate_root_causes
                )
            else:
                raise ValueError(f"Unknown analysis type: {analysis_type}")
            
            self.logger.info("Analysis completed with result: %s", result.root_causes)
            return result
            
        except (ValueError, TypeError, RuntimeError) as e:
            self.logger.error("Analysis failed: %s", e)
            return AnalysisResult(
                root_causes=[],
                confidence="low",
                evidence=False,
                error_message=f"Analysis failed: {e}"
            )
    
    def process_single_problem(self, problem_data: Dict[str, Any]) -> str:
        """
        处理单个问题
        
        Args:
            problem_data: 问题数据
            
        Returns:
            str: 根因候选
        """
        try:
            problem_id = problem_data.get("problem_id", "unknown")
            time_range = problem_data.get("time_range", "")
            candidate_root_causes = problem_data.get("candidate_root_causes", [])
            alarm_rules = problem_data.get("alarm_rules", [])
            
            self.logger.info("Processing problem %s", problem_id)
            self.logger.info("Time range: %s", time_range)
            self.logger.info("Alarm rules: %s", alarm_rules)
            self.logger.info("Candidates: %d possible root causes", len(candidate_root_causes))
            
            # 确定分析类型
            analysis_type = self.determine_analysis_type(alarm_rules)
            self.logger.info("Analysis type: %s", analysis_type.value)
            
            # 解析时间范围
            if " ~ " not in time_range:
                self.logger.error("Invalid time range format: %s", time_range)
                return "unknown"
            
            start_time, end_time = time_range.split(' ~ ')
            start_time = start_time.strip()
            end_time = end_time.strip()
            
            # 验证时间格式
            try:
                datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                self.logger.error("Invalid time format in: %s", time_range)
                return "unknown"
            
            # 运行分析
            result = self.run_analysis(analysis_type, start_time, end_time, candidate_root_causes)
            
            # 处理结果
            if result.is_success():
                root_cause = result.root_causes[0] if result.root_causes else "unknown"
                self.logger.info("Root cause found: %s", root_cause)
                return root_cause
            else:
                self.logger.warning("Analysis failed: %s", result.error_message)
                return "unknown"
            
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            self.logger.error("Failed to process problem %s: %s", 
                            problem_data.get('problem_id', 'unknown'), e)
            return "unknown"
    
    def process_all_problems(self, input_data: Optional[List[Dict[str, Any]]] = None,
                           input_file_path: str = "dataset/input.jsonl",
                           output_file_path: Optional[str] = None) -> List[str]:
        """
        处理所有问题
        
        Args:
            input_data: 预加载的输入数据（可选）
            input_file_path: 输入文件路径
            output_file_path: 输出文件路径
            
        Returns:
            List[str]: 处理结果列表
        """
        self.logger.info("Starting Root Cause Analysis")
        self.logger.info("=" * 60)
        
        # 读取输入数据
        if input_data is None:
            input_data = self.read_input_data(input_file_path)
        
        if not input_data:
            self.logger.error("No input data to process")
            return []
        
        # 处理每个问题
        results = []
        successful_analyses = 0
        failed_analyses = 0
        
        for i, problem_data in enumerate(input_data, 1):
            self.logger.info("Processing problem %d/%d", i, len(input_data))
            self.logger.info("-" * 40)
            
            result = self.process_single_problem(problem_data)
            results.append(result)
            
            if result != "unknown":
                successful_analyses += 1
            else:
                failed_analyses += 1
        
        # 总结
        self.logger.info("Analysis Summary")
        self.logger.info("=" * 60)
        self.logger.info("Total problems processed: %d", len(results))
        self.logger.info("Successful analyses: %d", successful_analyses)
        self.logger.info("Failed analyses: %d", failed_analyses)
        
        # 保存结果
        output_file = output_file_path or "dataset/output.jsonl"
        # 确保输出文件也使用绝对路径
        if not os.path.isabs(output_file):
            output_file = os.path.abspath(output_file)
        
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                for i, result in enumerate(results):
                    problem_id = input_data[i].get("problem_id", f"problem_{i + 1}")
                    root_causes = [result] if result and result != "unknown" else []
                    output_line = {"problem_id": problem_id, "root_causes": root_causes}
                    f.write(json.dumps(output_line, ensure_ascii=False) + "\n")
            self.logger.info("Results saved to: %s", output_file)
        except (IOError, OSError, TypeError) as e:
            self.logger.error("Failed to save results: %s", e)
        
        return results
    
    def run(self):
        """主执行方法"""
        self.logger.info("Starting root cause analysis...")
        
        # 加载环境变量
        self.logger.info("--- 开始加载环境变量 ---")
        if not load_env_variables():
            self.logger.error("❌ 环境变量加载失败，程序终止")
            return
        
        # 检查必需的环境变量
        missing_vars = self.config.validate_required_env_vars()
        if missing_vars:
            self.logger.error("❌ 缺少必需的环境变量: %s", ', '.join(missing_vars))
            return
        
        self.logger.info("✅ 所有必需的环境变量已设置")
        
        # 测试连接
        self.logger.info("--- 测试SLS连接 ---")
        if not sls_client_manager.test_connection():
            self.logger.warning("⚠️ SLS连接测试失败，但继续尝试运行分析")
            self.logger.warning("⚠️ 如果后续分析失败，请检查环境变量配置")
        else:
            self.logger.info("✅ SLS连接测试成功")
        
        # 处理数据
        input_data = self.read_input_data(self.file_name)
        results = self.process_all_problems(
            input_data=input_data, 
            output_file_path="dataset/output.jsonl"
        )
        
        # 打印总结
        self.logger.info("Final Results Summary:")
        self.logger.info("-" * 40)
        
        for i, result in enumerate(results):
            problem_id = input_data[i].get("problem_id", f"problem_{i+1}") if i < len(input_data) else f"problem_{i+1}"
            status = "SUCCESS" if result != "unknown" else "FAILED"
            self.logger.info("%s %s: %s", status, problem_id, result)
        
        self.logger.info("Analysis completed.")


if __name__ == "__main__":
    main = Main("dataset/test.jsonl")
    main.run()
