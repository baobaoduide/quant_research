import os
from pathlib import Path
from dotenv import load_dotenv

# ========================
# 1. 基础路径配置（完全移除QUANT_ROOT）
# ========================

# 核心原则：代码和数据分离但同级存放
# D盘结构：
#   D:\quant_research\        # 代码项目
#   D:\data_warehouse\        # 数据仓库（与代码项目同级）

# 获取项目根目录（代码所在位置）
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 数据仓库路径（与项目同级）
DATA_WAREHOUSE_ROOT = PROJECT_ROOT.parent.parent / "data_warehouse"

# 分层数据路径
L0_RAW_DATA = DATA_WAREHOUSE_ROOT / "L0_raw_data"
L1_PROCESSED_DATA = DATA_WAREHOUSE_ROOT / "L1_processed_data"

# 初始化存储工具
from core.data_utils.storage import DataStorage
l1_storage = DataStorage(L1_PROCESSED_DATA)

# 项目内路径
# CODE_WORKSPACE = PROJECT_ROOT / "code_workspace" # 有待修正
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
# RESULTS_DIR = PROJECT_ROOT / "results"

# ========================
# 2. 环境配置
# ========================

# 加载项目内的.env文件
load_dotenv(PROJECT_ROOT / ".env")

# 金融API密钥（从.env获取）
WIND_API_KEY = os.getenv("WIND_API_KEY")
GOAL_API_KEY = os.getenv("GOAL_API_KEY")

# ========================
# 3. 路径验证与创建
# ========================

def ensure_directory(path: Path):
    """确保目录存在，不存在则创建"""
    path.mkdir(parents=True, exist_ok=True)
    return path

# 初始化关键目录
ensure_directory(L0_RAW_DATA)
ensure_directory(L1_PROCESSED_DATA)
# ensure_directory(RESULTS_DIR)

# ========================
# 4. 路径获取工具函数
# ========================

def get_l0_path(source: str, data_type: str, date: str = None) -> Path:
    """构建L0原始数据路径"""
    base = ensure_directory(L0_RAW_DATA / source / data_type)
    return base / date if date else base

def get_l1_path(data_category: str, filename: str) -> Path:
    """构建L1处理数据路径"""
    dir_path = ensure_directory(L1_PROCESSED_DATA / data_category)
    return dir_path / filename

# ========================
# 5. 配置验证（可选）
# ========================

def validate_config():
    """配置验证，启动时调用"""
    assert L0_RAW_DATA.exists(), f"L0路径不存在: {L0_RAW_DATA}"
    assert L1_PROCESSED_DATA.exists(), f"L1路径不存在: {L1_PROCESSED_DATA}"
    assert PROJECT_ROOT != DATA_WAREHOUSE_ROOT, "项目和仓库路径不能相同!"

    print(f"项目根目录: {PROJECT_ROOT}")
    print(f"数据仓库根目录: {DATA_WAREHOUSE_ROOT}")

# 初始化时验证
if __name__ == "__main__":
    validate_config()