# scripts/build_seed_files.py
import csv
import io
from collections import Counter
from pathlib import Path

import httpx

MSA_COUNTIES = {
    "47037": ("Davidson", "Urban Core"),
    "47187": ("Williamson", "Williamson County"),
    "47149": ("Rutherford", "Rutherford County"),
    "47189": ("Wilson", "Wilson County"),
    "47165": ("Sumner", "Sumner County"),
}

DAVIDSON_REGIONS = {
    "Urban Core": {
        "37201", "37203", "37204", "37206", "37208", "37210", "37212",
        "37213", "37219", "37228", "37232", "37238", "37240", "37243", "37246"
    },
    "West Nashville": {
        "37205", "37209", "37215", "37220", "37221"
    },
    "North Nashville": {
        "37072", "37080", "37115", "37189", "37207", "37216", "37218"
    },
    "Southeast Nashville": {
        "37013", "37076", "37138", "37211", "37214", "37217"
    },
}

def get_davidson_region(zcta: str) -> str:
    for region, zips in DAVIDSON_REGIONS.items():
        if zcta in zips:
            return region
    return "Davidson Other"

def main():
    print("Downloading Census ZCTA-to-county relationship file...")
    url = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"
    response = httpx.get(url, timeout=60)
    response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text), delimiter="|")

    # Track best county per zip by largest INTERSECTION area (AREALAND_PART)
    # AREALAND_PART = land area of overlap between this ZCTA and this county
    # This is the correct field — AREALAND_ZCTA5_20 is the ZCTA's total area
    # and is identical across all county rows for the same zip, making it useless
    # for disambiguation.
    best_county: dict[str, tuple[str, int]] = {}

    for row in reader:
        zcta = row["GEOID_ZCTA5_20"].strip()
        county_geoid = row["GEOID_COUNTY_20"].strip()

        if county_geoid not in MSA_COUNTIES:
            continue

        area = int(row["AREALAND_PART"] or 0)

        if zcta not in best_county or area > best_county[zcta][1]:
            best_county[zcta] = (county_geoid, area)

    # Build seed rows from deduplicated zip→county mapping
    valid_zips = []
    zip_regions = []

    for zcta, (county_geoid, _) in sorted(best_county.items()):
        county_name, default_region = MSA_COUNTIES[county_geoid]

        if county_geoid == "47037":
            region = get_davidson_region(zcta)
        else:
            region = default_region

        valid_zips.append({"zip_code": zcta})
        zip_regions.append({
            "zip_code": zcta,
            "nashville_region": region,
            "county_name": county_name,
            "county_fips": county_geoid,
        })

    # Write seeds
    seeds_dir = Path("housing_pipeline/seeds")
    seeds_dir.mkdir(exist_ok=True)

    with open(seeds_dir / "nashville_valid_zips.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["zip_code"])
        writer.writeheader()
        writer.writerows(valid_zips)

    with open(seeds_dir / "nashville_zip_regions.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["zip_code", "nashville_region", "county_name", "county_fips"])
        writer.writeheader()
        writer.writerows(zip_regions)

    print(f"Done. {len(valid_zips)} MSA zips written.")
    print("Breakdown:")
    counts = Counter(r["county_name"] for r in zip_regions)
    for county, count in sorted(counts.items()):
        print(f"  {county}: {count} zips")

if __name__ == "__main__":
    main()