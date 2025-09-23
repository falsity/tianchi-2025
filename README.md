# Tianchi 2025 AIOps Challenge

本repo是2025 AI原生编程挑战赛的示例代码仓库 （不定期更新）

[比赛页面链接](https://tianchi.aliyun.com/competition/entrance/532387?utm_content=g_1000406886)

## Root Cause Analysis Tool

### Input Data

Place your input data in `dataset/input.jsonl` following the required format.

### Environment Variables

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="your-access-key-id"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="your-access-key-secret"
export ALIBABA_CLOUD_ROLE_ARN="acs:ram::1672753017899339:role/tianchi-user-a"
export ALIBABA_CLOUD_ROLE_SESSION_NAME="my-sls-access"
```

### Running the Analysis

Execute the analysis script:

```bash
./run_analysis.sh
```

### Output Results

Results will be generated in `dataset/output.jsonl` with the identified root causes for each problem case.
