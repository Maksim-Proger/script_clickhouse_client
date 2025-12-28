from project.nats_corn.parser import parse_input


def main():
    test_data = """
    192.168.0.1 10.0.0.2
    172.16.0.5

    {"ip": "8.8.8.8"}
    {"meta": {"ip": "1.1.1.1"}}

    [
        {"ip": "9.9.9.9"},
        {"nested": {"ip": "4.4.4.4"}}
    ]
    """

    ips = parse_input(test_data)
    print(";".join(ips))


if __name__ == "__main__":
    main()
