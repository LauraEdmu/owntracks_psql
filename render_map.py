#!/usr/bin/env python3

# dump all location data to CSV with:
# COPY (
#     SELECT
#         tst,
#         lat,
#         lon,
#         acc,
#         batt,
#         vel
#     FROM owntracks_locations
#     ORDER BY tst
# ) TO '/tmp/locations.csv' CSV HEADER;

# after running this script run a small web server to serve the generated HTML files, e.g.:
# python3 -m http.server 8080

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any

import folium
from folium.plugins import HeatMap, TimestampedGeoJson


def load_points(csv_path: Path) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                lat = float(row["lat"])
                lon = float(row["lon"])
            except (KeyError, TypeError, ValueError):
                continue

            tst = str(row.get("tst", "")).strip()
            acc = str(row.get("acc", "")).strip()
            batt = str(row.get("batt", "")).strip()
            vel = str(row.get("vel", "")).strip()

            points.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "tst": tst,
                    "acc": acc,
                    "batt": batt,
                    "vel": vel,
                }
            )

    return points


def centre_for_points(points: list[dict[str, Any]]) -> tuple[float, float]:
    avg_lat = sum(p["lat"] for p in points) / len(points)
    avg_lon = sum(p["lon"] for p in points) / len(points)
    return avg_lat, avg_lon


def build_hover_map(points: list[dict[str, Any]], output_path: Path) -> None:
    centre_lat, centre_lon = centre_for_points(points)

    m = folium.Map(
        location=[centre_lat, centre_lon],
        zoom_start=14,
        tiles="OpenStreetMap",
    )

    path = [(p["lat"], p["lon"]) for p in points]

    folium.PolyLine(
        path,
        weight=4,
        opacity=0.85,
        tooltip="Tracked path",
    ).add_to(m)

    folium.Marker(
        path[0],
        popup=f"Start<br>{points[0]['tst']}",
        tooltip="Start",
        icon=folium.Icon(color="green"),
    ).add_to(m)

    folium.Marker(
        path[-1],
        popup=f"End<br>{points[-1]['tst']}",
        tooltip="End",
        icon=folium.Icon(color="red"),
    ).add_to(m)

    for p in points:
        popup_html = (
            f"<b>Time:</b> {p['tst'] or 'unknown'}<br>"
            f"<b>Lat:</b> {p['lat']:.7f}<br>"
            f"<b>Lon:</b> {p['lon']:.7f}<br>"
            f"<b>Accuracy:</b> {p['acc'] or 'unknown'}<br>"
            f"<b>Battery:</b> {p['batt'] or 'unknown'}<br>"
            f"<b>Speed:</b> {p['vel'] or 'unknown'}"
        )

        tooltip_text = p["tst"] or f"{p['lat']:.5f}, {p['lon']:.5f}"

        folium.CircleMarker(
            location=(p["lat"], p["lon"]),
            radius=3,
            weight=1,
            fill=True,
            fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=tooltip_text,
        ).add_to(m)

    m.save(str(output_path))


def build_animated_map(points: list[dict[str, Any]], output_path: Path) -> None:
    centre_lat, centre_lon = centre_for_points(points)

    m = folium.Map(
        location=[centre_lat, centre_lon],
        zoom_start=14,
        tiles="OpenStreetMap",
    )

    features: list[dict[str, Any]] = []

    for p in points:
        if not p["tst"]:
            continue

        popup_html = (
            f"<b>Time:</b> {p['tst']}<br>"
            f"<b>Lat:</b> {p['lat']:.7f}<br>"
            f"<b>Lon:</b> {p['lon']:.7f}"
        )

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [p["lon"], p["lat"]],
                },
                "properties": {
                    "time": p["tst"],
                    "popup": popup_html,
                    "icon": "circle",
                    "iconstyle": {
                        "fillOpacity": 0.9,
                        "stroke": True,
                        "radius": 5,
                    },
                },
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    TimestampedGeoJson(
        data=geojson,
        period="PT1M",
        duration="PT1M",
        auto_play=False,
        loop=False,
        max_speed=20,
        loop_button=True,
        date_options="YYYY-MM-DD HH:mm:ss",
        time_slider_drag_update=True,
    ).add_to(m)

    m.save(str(output_path))


def build_heatmap(points: list[dict[str, Any]], output_path: Path) -> None:
    centre_lat, centre_lon = centre_for_points(points)

    m = folium.Map(
        location=[centre_lat, centre_lon],
        zoom_start=13,
        tiles="OpenStreetMap",
    )

    heat_data = [[p["lat"], p["lon"]] for p in points]

    HeatMap(
        heat_data,
        radius=18,
        blur=14,
        min_opacity=0.35,
    ).add_to(m)

    m.save(str(output_path))


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {Path(__file__).name} <csv_file>")
        sys.exit(1)

    csv_path = Path(sys.argv[1])

    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        sys.exit(1)

    points = load_points(csv_path)

    if not points:
        print("No valid points found in CSV.")
        sys.exit(1)

    hover_output = Path("map_basic_hover.html")
    animated_output = Path("map_animated.html")
    heatmap_output = Path("map_heatmap.html")

    build_hover_map(points, hover_output)
    build_animated_map(points, animated_output)
    build_heatmap(points, heatmap_output)

    print(f"Wrote {hover_output}")
    print(f"Wrote {animated_output}")
    print(f"Wrote {heatmap_output}")


if __name__ == "__main__":
    main()