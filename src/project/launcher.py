from project.nats_corn.main import main as nats_corn_main
from project.script_ch_client.main import main as ch_client_main
from project.script_ch_writer.main import main as ch_writer_main


def main() -> None:
    nats_corn_main()
    ch_client_main()
    ch_writer_main()
