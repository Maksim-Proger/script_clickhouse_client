import time

from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.http.src2_client import DgClient
from project.nats_corn.parser.parser import parse_input

AB_INTERVAL: float = 20.0
DG_INTERVAL: float = 4 * 60.0

def main():
    ab_client = AbClient()
    dg_client = DgClient()

    last_dg_run = 0.0

    while True:
        now = time.time()

        # AB — каждые 20 секунд
        try:
            raw_data_ab = ab_client.get_data()
            ips_ab = parse_input(raw_data_ab)
            print("AB:", ";".join(ips_ab))
        except Exception as e:
            print(f"AB error: {e}")

        # DG — каждые 4 минуты
        if now - last_dg_run >= DG_INTERVAL:
            try:
                raw_data_dg = dg_client.get_data()
                ips_dg = parse_input(raw_data_dg)
                print("DG:", ";".join(ips_dg))
                last_dg_run = now
            except Exception as e:
                print(f"DG error: {e}")

        time.sleep(AB_INTERVAL)

if __name__ == "__main__":
    main()
