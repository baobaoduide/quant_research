from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).parent.parent

# 数据目录
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
MODEL_DATA_DIR = DATA_DIR / "model"

# 结果目录
RESULTS_DIR = BASE_DIR / "results"
FIGURES_DIR = RESULTS_DIR / "figures"
TABLES_DIR = RESULTS_DIR / "tables"
MODELS_DIR = RESULTS_DIR / "models"

# 模型参数
GK_MODEL_PARAMS = {
    'lookback_period': 5,    # 回溯年数
    'return_window': 12,      # 回报计算窗口（月）
    'min_market_cap': 1e9,    # 最小市值过滤（10亿）
}

# 数据范围
START_DATE = '20100101'
END_DATE = '20231231'