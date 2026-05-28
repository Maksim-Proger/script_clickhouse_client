import logging
from typing import Optional

import geoip2.database
import geoip2.errors

logger = logging.getLogger(__name__)


class GeoIPClient:
    def __init__(self, country_db_path: str, asn_db_path: str):
        self._country_db_path = country_db_path
        self._asn_db_path = asn_db_path
        self._country_reader: Optional[geoip2.database.Reader] = None
        self._asn_reader: Optional[geoip2.database.Reader] = None

    def open(self) -> None:
        self._country_reader = geoip2.database.Reader(self._country_db_path)
        self._asn_reader = geoip2.database.Reader(self._asn_db_path)
        logger.info("action=geoip_open status=success")

    def close(self) -> None:
        if self._country_reader:
            self._country_reader.close()
            self._country_reader = None
        if self._asn_reader:
            self._asn_reader.close()
            self._asn_reader = None
        logger.info("action=geoip_close status=success")

    def _lookup_ip(self, ip: str) -> dict:
        country, asn_number, asn_org = None, None, None

        if self._country_reader:
            try:
                r = self._country_reader.country(ip)
                country = r.country.iso_code
            except (geoip2.errors.AddressNotFoundError, Exception):
                pass

        if self._asn_reader:
            try:
                r = self._asn_reader.asn(ip)
                asn_number = r.autonomous_system_number
                asn_org = r.autonomous_system_organization
            except (geoip2.errors.AddressNotFoundError, Exception):
                pass

        return {
            "country": country,
            "asn_number": asn_number,
            "asn_org": asn_org,
        }

    def enrich_batch(self, records: list[dict]) -> list[dict]:
        for record in records:
            geo = self._lookup_ip(record["ip_address"])
            record.update(geo)
        return records