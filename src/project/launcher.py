from multiprocessing import Process
from project.nats_corn.main import main as nats_corn_main
from project.script_ch_client.main import main as ch_client_main
from project.script_ch_writer.main import main as ch_writer_main
from project.common.config_loader.loader import load_yaml


CONFIG = load_yaml("src/project/config/nats_corn.yaml")

def main() -> None:
    processes = [
        Process(target=nats_corn_main, args=(CONFIG,), name="nats-corn"),
        Process(target=ch_client_main,  args=(CONFIG,), name="ch-client"),
        Process(target=ch_writer_main,  args=(CONFIG,), name="ch-writer"),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()
