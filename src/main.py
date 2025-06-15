import datetime
import json
import random
import time
import csv
import os

import requests
from requests.exceptions import InvalidJSONError, RequestException, Timeout, HTTPError
from dotenv import load_dotenv
import structlog

log = structlog.get_logger()

load_dotenv(dotenv_path="./headers.config.env")

HEADERS = {
    "Apikey": os.getenv("APIKEY"),
    "Mobile-Platform": os.getenv("MOBILE_PLATFORM"),
    "User_id": os.getenv("USER_ID"),
    "App_version": os.getenv("APP_VERSION"),
    "Mobile-Version": os.getenv("MOBILE_VERSION"),
    "Mobile-Version-Os": os.getenv("MOBILE_VERSION_OS"),
    "Mobile-Build": os.getenv("MOBILE_BUILD"),

    "Host": "mobile.api-lmn.ru",
    "Ab-Test-Option": "opt1",
    "User-Agent": "ktor-client",
    "Plp-Srp-View": "mixed",
    "Pdp-Content-Ab-Option": "all",
    "Content-Length": "232",
    "Accept-Language": "ru",
    "Accept-Charset": "UTF-8",
    "Accept": "application/json",
    "Content-Type": "application/json; charset=UTF-8",
    "Accept-Encoding": "gzip, deflate, br",
}


class LemanaProItemParser:
    """API parser for Lemana Pro"""
    search_url = "https://mobile.api-lmn.ru/mobile/v2/search"

    def __init__(
            self,
            headers: dict[str, str],
            *,
            output_filename: str = "lemana_positions"
    ):
        self.output_filename = output_filename
        self.session = requests.Session()
        self.session.headers.update(headers)

    def scrape(
            self,
            catalogue_item: str,
            region_id: int,
            *,
            only_available: bool = True,
            show_services: bool = False,
            show_facets: bool = False,
            start_page: int = 1,
            timeout_retries: int = 3,
    ):
        """
        Main function that responsible for requesting and parsing data from mobile app API of Lemana Pro

        :param catalogue_item: Name of the category in catalogue we want to parse
        :param region_id: Id of the region from which to parse available items
        :param only_available: Flag if we want to parse only available in regions positions
        :param show_services: Flag to show services in json response of the API
        :param show_facets: Same as services but for facets
        :param start_page: From which page to start querying, useful when checkpoint has been created already
        :param timeout_retries: Number of retries on timeout before stopping requests
        :return:
        """
        search_body = self._create_search_body(
            catalogue_item=catalogue_item,
            region_id=region_id,
            only_available=only_available,
            show_services=show_services,
            show_facets=show_facets,
        )
        page_counter = start_page

        with open(f"{self.output_filename}.csv", "w") as output:
            data_writer = csv.writer(output, delimiter=";")
            data_writer.writerow(["id", "name", "brand", "regular_price", "discount_price"])

            while True:
                offset = (page_counter - 1) * 30
                search_body["limitFrom"] = offset

                try:
                    response = self.session.post(url=self.search_url, json=search_body)
                    headers = response.headers
                    body = response.json()
                except (InvalidJSONError, Timeout, HTTPError, RequestException) as err:
                    if isinstance(err, InvalidJSONError):
                        log.error("Something wrong with response json", err, response.status_code)

                    elif isinstance(err, Timeout):
                        log.warn("Too much time has passed, retrying", err, response.status_code)
                        time.sleep(20)
                        if timeout_retries > 0:
                            continue

                    elif isinstance(err, HTTPError):
                        log.error("Wrong response status", err, response.status_code)

                    elif isinstance(err, RequestException):
                        log.error("Something with request", err, response.status_code)

                    self._create_checkpoint(reason=err, status=response.status_code, page=page_counter)
                    break
                except Exception as err:
                    log.error("Unknown issue", err, response.status_code)
                    break

                items = body.get("items", [])
                total_item_count = body.get('items_cnt')
                item_array_length = len(items)

                rate_limit_remaining = int(headers.get("RateLimit-Remaining"))
                secs_until_reset = int(headers.get("RateLimit-Reset"))

                # while API is being scraped, Lemana can change their records,
                # and then it creates issues in pagination for us
                if offset > total_item_count:
                    break

                log.info(
                    f"-STATS-\n"
                    f"TOTAL_ITEMS: {total_item_count}\n"
                    f"ITEMS_IN_RESPONSE: {item_array_length}\n"
                    f"CURRENT_PAGE: {page_counter}\n"
                    f"PROGRESS: {((item_array_length * page_counter / total_item_count) * 100):.2f}%\n"
                    f"RATE_LIMIT_REMAINING: {rate_limit_remaining}\n"
                    f"SECS_UNTIL_LIMIT_RESET: {secs_until_reset}\n"
                )

                for item in items:
                    id = item.get("articul")
                    name = item.get("displayedName")
                    brand = item.get("brand")

                    main_price = None
                    old_price = None

                    for price in item.get("prices"):
                        if price.get("type") == "displayMain":
                            main_price = price.get("price")
                        elif price.get("type") == "displayOld":
                            old_price = price.get("price")

                    regular_price = None
                    discount_price = None

                    if old_price is not None:
                        regular_price = old_price
                        discount_price = main_price
                    else:
                        regular_price = main_price

                    data_writer.writerow([id, name, brand, regular_price, discount_price])

                if rate_limit_remaining < 5000:
                    log.info("WE NEED TO WAIT TO RESET LIMITER")
                    time.sleep(secs_until_reset + 10)

                page_counter += 1
                time.sleep(random.random() * 10)

    def _create_search_body(
            self,
            catalogue_item: str,
            region_id: int,
            *,
            only_available: bool = True,
            show_services: bool = False,
            show_facets: bool = False
    ):
        """Creates request body for search endpoint"""
        search_body = {
            "familyId": "",
            "limitCount": 30,
            "limitFrom": 0,
            "regionsId": region_id,
            "availability": only_available,
            "showProducts": True,
            "showFacets": show_facets,
            "showServices": show_services,
            "sitePath": f"/catalogue/{catalogue_item}/"
        }

        return search_body

    def _create_checkpoint(self, *, reason: Exception, status: int, page: int):
        """Creates checkpoint in case of error in request, so you can continue from last page"""
        data = {
            "time": datetime.datetime.now(),
            "status_code": status,
            "reason": reason,
            "last_page": page
        }

        with open("checkpoint.json", "w") as file:
            json.dump(data, file)

        log.info("CHECKPOINT CREATED")


# region id
# 34 Москва и Мос.область
# 506 Санкт-Петербург
if __name__ == "__main__":
    scraper = LemanaProItemParser(headers=HEADERS, output_filename="saint_petersburg_keramogranit")
    scraper.scrape(catalogue_item="keramogranit", region_id=506)
