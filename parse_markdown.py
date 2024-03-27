import dataclasses
from datetime import datetime, time

from bs4 import BeautifulSoup, Tag

import markdown

from matplotlib import pyplot
import networkx


@dataclasses.dataclass(frozen=True)
class Stop:
    arrival: time
    departure: time
    short_name: str
    full_name: str
    pois: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Route:
    train_name: str
    start: str
    intermediate_stops: tuple[Stop, ...]
    end: str


def parse_stops(description: str, item_pois: set[str]) -> Stop:
    times, name = description.split(sep=" - ", maxsplit=1)
    departure, arrival = times.split(sep=" | ", maxsplit=1)
    short_name = name[-4:-1]
    full_name = name[:-6]

    departure = departure.replace("a", "AM").replace("p", "PM")
    arrival = arrival.replace("a", "AM").replace("p", "PM")

    dep = datetime.strptime(departure, '%I:%M%p')
    arr = datetime.strptime(arrival, '%I:%M%p')

    return Stop(
        departure=dep.time(),
        arrival=arr.time(),
        short_name=short_name,
        full_name=full_name,
        pois=tuple(item_pois)
    )


def parse_train_routes(input_file: str) -> set[Route]:
    with open(input_file, mode="r") as file:
        route_markdown = file.read()

    html = markdown.markdown(route_markdown)

    soup = BeautifulSoup(html, features="html.parser")

    trains = set()

    segment = None
    segment_start = None
    segment_end = None
    for each_child in soup.children:
        if isinstance(each_child, Tag) and each_child.name == "h2":
            segment = each_child.get_text(strip=True)
            if " -> " not in segment:
                continue
            segment_start, segment_end = segment.split(sep=" -> ", maxsplit=1)
            continue

        if not isinstance(each_child, Tag) or not each_child.name == "ul":
            continue

        for each_li in each_child.children:
            if not isinstance(each_li, Tag) or not each_li.name == "li":
                continue
            each_train = each_li.contents[0]
            train_name = each_train.get_text(strip=True)
            if len(train_name) < 1:
                continue

            ordered_list_distances = each_li.contents[1]
            if not isinstance(ordered_list_distances, Tag) or not ordered_list_distances.name == "ol":
                continue

            print(f"train {train_name}")
            stops = list()
            for each_item in ordered_list_distances.children:
                if not isinstance(each_item, Tag) or not each_item.name == "li":
                    continue
                each_pois = set()

                each_description = each_item.contents[0].get_text(strip=True)
                unordered_list = each_item.contents[1]

                if isinstance(unordered_list, Tag) and unordered_list.name == "ul":
                    item_pois = unordered_list.contents[1]
                    if isinstance(item_pois, Tag) and item_pois.name == "li":
                        c = list(item_pois.children)
                        poi_items = c[1]
                        if isinstance(poi_items, Tag) and poi_items.name == "ul":
                            for each_poi in poi_items.contents:
                                if not isinstance(each_poi, Tag) or not each_poi.name == "li":
                                    continue
                                each_pois.add(each_poi.get_text(strip=True))

                each_stop = parse_stops(each_description, each_pois)
                stops.append(each_stop)

            route = Route(
                train_name=train_name,
                start=segment_start,
                intermediate_stops=tuple(stops),
                end=segment_end
            )
            print(route)
            print()
            trains.add(route)

    return trains


def main() -> None:
    trains = parse_train_routes("markdown_files/routes.md")
    # print(trains)
    g = networkx.Graph()
    for each_route in trains:
        stops = each_route.intermediate_stops
        len_route = len(stops)
        for i in range(len_route - 1):
            g.add_edge(stops[i].short_name, stops[i + 1].short_name)

    networkx.draw(g, with_labels=True, node_color='lightblue', edge_color='gray', node_size=2000, font_size=10)
    pyplot.title("My DataClass Graph")
    pyplot.show()


if __name__ == "__main__":
    main()
