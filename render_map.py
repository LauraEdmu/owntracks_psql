#!/usr/bin/env python3

# First make a dump like this from your database (adjusting the query as needed):
# COPY (
#     SELECT
#         tst,
#         lat,
#         lon
#     FROM owntracks_locations
#     ORDER BY tst
# ) TO '/tmp/locations.csv' CSV HEADER;

import csv
import sys
from pathlib import Path

import folium


def load_points(csv_path: Path):
    points = []

    with csv_path.open() as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                lat = float(row["lat"])
                lon = float(row["lon"])
                tst = row.get("tst", "")

                points.append((lat, lon, tst))
            except (ValueError, KeyError):
                continue

    return points


def main():
    if len(sys.argv) != 2:
        print("Usage: map_from_csv.py data.csv")
        sys.exit(1)

    csv_path = Path(sys.argv[1])

    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        sys.exit(1)

    points = load_points(csv_path)

    if not points:
        print("No valid points found")
        sys.exit(1)

    # Center map on first point
    start_lat, start_lon, _ = points[0]

    m = folium.Map(
        location=[start_lat, start_lon],
        zoom_start=13,
        tiles="OpenStreetMap",
    )

    # Draw path
    path = [(lat, lon) for lat, lon, _ in points]

    folium.PolyLine(
        path,
        weight=4,
        opacity=0.8,
    ).add_to(m)

    # Add start marker
    folium.Marker(
        path[0],
        popup="Start",
        icon=folium.Icon(color="green"),
    ).add_to(m)

    # Add end marker
    folium.Marker(
        path[-1],
        popup="End",
        icon=folium.Icon(color="red"),
    ).add_to(m)

    # Add small markers for points
    for lat, lon, tst in points:
        folium.CircleMarker(
            location=(lat, lon),
            radius=2,
            popup=tst,
        ).add_to(m)

    output = "map.html"
    m.save(output)

    print(f"Map written to {output}")


if __name__ == "__main__":
    main()