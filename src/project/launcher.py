from multiprocessing import Process
from project.module_data_collector.main import main as data_collector_main
from project.module_ch_api_gateway.main import main as ch_api_gateway_main
from project.module_ch_loader.main import main as ch_loader_main
from project.utils.config_loader.loader import load_yaml


CONFIG_DATA_COLLECTOR = load_yaml("src/project/config/module_data_collector.yaml")
CONFIG_CH_API_GATEWAY = load_yaml("src/project/config/module_ch_api_gateway.yaml")
CONFIG_CH_LOADER = load_yaml("src/project/config/module_ch_loader.yaml")

DG_CONFIG = load_yaml("src/project/config/dg_sources.yaml")

CONFIG_DATA_COLLECTOR["dg_sources"] = DG_CONFIG.get("dg_sources", [])
CONFIG_DATA_COLLECTOR["dg_defaults"] = DG_CONFIG.get("dg_defaults", {})

def main() -> None:
    processes = [
        Process(target=data_collector_main, args=(CONFIG_DATA_COLLECTOR,), name="data_collector"),
        Process(target=ch_api_gateway_main, args=(CONFIG_CH_API_GATEWAY,), name="ch_api_gateway"),
        Process(target=ch_loader_main, args=(CONFIG_CH_LOADER,), name="ch_loader"),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()
