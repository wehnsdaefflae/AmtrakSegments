import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

import dateparser
import httpx
import pandas

import tabula
from pandas import DataFrame


async def download_pdfs(target_directory: str) -> None:
    assert target_directory.endswith("/"), "Target directory must end with a slash"

    url = "https://content.amtrak.com/content/timetable/{route_name}.pdf"
    routes = (
        'Acela', 'Adirondack', 'Amtrak Cascades', 'Amtrak Hartford Line', 'Auto Train', 'Berkshire Flyer',
        'Blue Water', 'California Zephyr', 'Capitol Corridor', 'Capitol Limited', 'Cardinal', 'Carl Sandburg',
        'Carolinian', 'City of New Orleans', 'Coast Starlight', 'Crescent', 'Downeaster', 'Empire Builder',
        'Empire Service', 'Ethan Allen Express', 'Heartland Flyer', 'Hiawatha Service', 'Illini', 'Illinois Zephyr',
        'Keystone Service', 'Lake Shore Limited', 'Lincoln Service', 'Lincoln Service Missouri River Runner',
        'Maple Leaf', 'Missouri River Runner', 'Northeast Regional', 'Pacific Surfliner', 'Palmetto', 'Pennsylvanian',
        'Pere Marquette', 'Piedmont', 'Saluki', 'San Joaquins', 'Silver Meteor', 'Southwest Chief', 'Sunset Limited',
        'Texas Eagle', 'Valley Flyer', 'Vermonter', 'Wolverine'
    )

    httpx_client = httpx.AsyncClient()
    tasks = list()
    for route in routes:
        tasks.append(httpx_client.get(url.format(route_name=route), timeout=10))

    responses = await asyncio.gather(*tasks)
    for response, route in zip(responses, routes):
        if response.status_code == 200:
            with open(f"{target_directory}{route}.pdf", mode="wb") as pdf_file:
                pdf_file.write(response.content)
        else:
            print(f"Failed to download {route}.pdf")

    await httpx_client.aclose()


def extract_station_name(cell: str) -> None | str:
    # extract all substrings that are three letters max and in brackets
    station_names = re.findall(r"\((\w{1,3})\)", cell)
    number_of_station_names = len(station_names)
    if number_of_station_names < 1:
        return None
    if number_of_station_names == 1:
        return station_names[0]
    raise ValueError(f"Multiple station names in line: {station_names}")


def cancel(line: list[str]) -> bool:
    return "Operated by" in line[0]


@dataclass
class Stop:
    short_name: str
    full_name: str
    departure: datetime


ServiceNumber = str


@dataclass
class ServiceHeader:
    number: ServiceNumber
    route: str
    days: list[str]
    duration: timedelta


@dataclass
class Service:
    service_header: ServiceHeader
    stops: list[Stop]


@dataclass
class TrainHeader:
    name: str
    last_updated: datetime


@dataclass
class Train:
    header: TrainHeader
    services: dict[ServiceNumber, Service]


def get_service_indices(line: list[str]) -> None | dict[str, int]:
    if "Service Number" not in line:
        return None

    indices = dict()
    for index, each_cell in enumerate(line):
        if each_cell.isnumeric():
            indices[each_cell] = index

    return indices


def get_route_name(line: list[str], service_index: int) -> None | str:
    if "Route" not in line:
        return None

    return line[service_index]


def get_days(line: list[str], service_index: int) -> None | list[datetime.weekday]:
    if "Days of Operation" not in line:
        return None

    days_string = line[service_index].strip()
    if days_string == "Mo-Fr":
        return list(range(0, 5))

    if days_string == "SuSa":
        return [7, 6]

    raise ValueError(f"Unknown days of operation: {days_string}")


def get_duration(line: list[str], service_index: int) -> None | timedelta:
    if "Duration" not in line:
        return None

    duration_string = line[service_index].strip()
    duration = datetime.strptime(duration_string, "%Hh %Mm")
    return timedelta(hours=duration.hour, minutes=duration.minute)


def is_bidirectional(line: list[str]) -> None | bool:
    if "Read Direction" not in line:
        return None

    return "Bidirectional" in line


def get_stop(line: list[str], service_index: int, table: list[list[str]]) -> None | Stop:
    pass


def get_train_header(table: list[list[str]]) -> TrainHeader:
    name_train = table[0][-1]

    date_str = table[1][0]
    day_removed = date_str.split(sep=",", maxsplit=1)[1].strip()
    # parse "March 26, 2024"
    last_updated = dateparser.parse(day_removed, languages=["en"], locales=["US"])

    return TrainHeader(name=name_train, last_updated=last_updated)


def parse_pdf(pdf_path: str) -> str:
    dataframes: list[DataFrame] = tabula.read_pdf(pdf_path, pages="all")

    rows = list()
    for each_dataframe in dataframes:
        rows.extend(each_dataframe.values.tolist())

    services = dict()

    train_header = get_train_header(rows)

    current_service_indices = None
    for index_row, each_row in enumerate(rows):
        if current_service_indices is None:
            service_indices = get_service_indices(each_row)
            if service_indices is not None:
                current_service_indices = service_indices
                continue

        for each_service_number, each_service_index in current_service_indices.items():
            each_route_name = get_route_name(each_row, each_service_index)
            if each_route_name is not None:
                each_service = Service(
                    train=each_route_name,
                    number=each_service_number,
                    days=get_days(each_row, each_service_index),
                    stops=[],
                )

                services.append(each_service)


def make_tsv(pdf_path: str) -> None:
    file_name = os.path.basename(pdf_path).replace(".pdf", ".tsv")
    os.makedirs("tsv", exist_ok=True)
    tabula.convert_into(pdf_path, f"tsv/{file_name}", output_format="tsv", pages="all", guess=False)


def main() -> None:
    # asyncio.run(download_pdfs("pdf/"))
    # parse_pdf("pdf/Acela.pdf")
    make_tsv("pdf/Valley Flyer.pdf")


if __name__ == "__main__":
    main()
