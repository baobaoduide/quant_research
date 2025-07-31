from core.data_utils.loader import DataLoader
from core.data_utils.storage import DataStorage


DATA_LOADER = DataLoader()
ashare_info = DATA_LOADER.load_l2(project='gk_model', table='ashare_base_info')
