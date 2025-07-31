# core/data_utils/__init__.py
from .l0_loader import L0Loader

# 创建默认实例
l0_loader = L0Loader(cache_enabled=True)