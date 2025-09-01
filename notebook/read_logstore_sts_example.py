# -*- coding: utf-8 -*-
import time
import os
from datetime import datetime, timedelta
from aliyun.log import LogClient, GetLogsRequest
from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_credentials.models import Config
from alibabacloud_sts20150401.client import Client as Sts20150401Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_sts20150401 import models as sts_20150401_models
from alibabacloud_tea_util import models as util_models

def get_sts_token():
    """
    获取STS临时访问凭证
    """
    config = Config(
        type='access_key',
        access_key_id=os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID', ''),
        access_key_secret=os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET', '')
    )
    credential = CredentialClient(config)
    sts_config = open_api_models.Config(
        credential=credential
    )
    sts_config.endpoint = 'sts.cn-qingdao.aliyuncs.com'
    client = Sts20150401Client(sts_config)
    
    assume_role_request = sts_20150401_models.AssumeRoleRequest(
        role_arn="acs:ram::1555511022232981:role/test-to-liye-personal",
        role_session_name="test-to-liye-personal",
        duration_seconds=3600
    )
    runtime = util_models.RuntimeOptions()
    response = client.assume_role_with_options(assume_role_request, runtime)
    return response.body.credentials

def get_recent_logs():
    """
    获取最近1分钟的日志数据
    """
    # 配置日志服务的访问参数
    endpoint = 'cn-qingdao.log.aliyuncs.com'  
    project = 'cms-tianchi-2025-test-sls-5cmpninmfzqtdrqy'  # 替换为您的Project名称
    logstore = 'tianchi-2025-test-sls__entity'  # 替换为您的Logstore名称
    
    # 计算时间范围
    end_time = int(time.time())
    start_time = end_time - 60  # 60秒 = 1分钟
    
    try:
        # 获取STS临时凭证
        sts_token = get_sts_token()
        
        # 创建LogClient实例,使用STS临时凭证
        client = LogClient(endpoint,
                         sts_token.access_key_id,
                         sts_token.access_key_secret,
                         securityToken=sts_token.security_token)
        
        # 构建日志查询请求
        request = GetLogsRequest(project, logstore,
                               fromTime=start_time,
                               toTime=end_time,
                               topic='',
                               query="* | SELECT * ORDER BY __time__ DESC")  # 可以根据需要修改查询语句
        
        print(f"正在查询日志...")
        print(f"时间范围: {datetime.fromtimestamp(start_time)} 到 {datetime.fromtimestamp(end_time)}")
        
        # 执行查询
        response = client.get_logs(request)
        
        if response.get_count() > 0:
            print(f"\n找到 {response.get_count()} 条日志记录:")
            for log in response.get_logs():
                print(f"\n时间: {datetime.fromtimestamp(log.get_time())}")
                for key, value in log.get_contents().items():
                    print(f"{key}: {value}")
        else:
            print("未找到任何日志记录")
            
    except Exception as e:
        print(f"查询日志失败: {e}")
        raise

def main():
    """
    主函数
    """
    print("=== 日志查询测试 ===")
    
    try:
        get_recent_logs()
    except Exception as e:
        print(f"程序执行失败: {e}")
        return

if __name__ == '__main__':
    main()
