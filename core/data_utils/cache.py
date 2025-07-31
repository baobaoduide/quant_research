# core/data_utils/cache.py
class DataCache:
	"""简化的缓存实现"""
	
	def __init__(self):
		self.cache = {}
	
	def get(self, key: str):
		return self.cache.get(key)
	
	def set(self, key: str, value):
		self.cache[key] = value