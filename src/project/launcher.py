from multiprocessing import Process
from project.nats_corn.main import main as nats_corn_main
from project.script_ch_client.main import main as ch_client_main
from project.script_ch_writer.main import main as ch_writer_main
from project.common.config_loader.loader import load_yaml


CONFIG_NATS_CORN = load_yaml("src/project/config/nats_corn.yaml")
CONFIG_CH_CLIENT = load_yaml("src/project/config/script_ch_client.yaml")
CONFIG_CH_WRITER = load_yaml("src/project/config/script_ch_writer.yaml")

DG_SOURCES = load_yaml("src/project/config/dg_sources.yaml")

CONFIG_NATS_CORN["dg_sources"] = DG_SOURCES.get("dg_sources", [])

def main() -> None:
    processes = [
        Process(target=nats_corn_main, args=(CONFIG_NATS_CORN,), name="nats-corn"),
        Process(target=ch_client_main, args=(CONFIG_CH_CLIENT,), name="ch-client"),
        Process(target=ch_writer_main, args=(CONFIG_CH_WRITER,),name="ch-writer"),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()
