from multiprocessing import Process
from project.nats_corn.main import main as nats_corn_main
from project.script_ch_client.main import main as ch_client_main
from project.script_ch_writer.main import main as ch_writer_main


def main() -> None:
    processes = [
        Process(target=nats_corn_main, name="nats-corn"),
        Process(target=ch_client_main, name="ch-client"),
        Process(target=ch_writer_main, name="ch-writer"),
    ]

    for p in processes:
        p.start()

    for p in processes:
        p.join()
