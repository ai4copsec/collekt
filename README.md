# collekt: a library to facilitate spatio-temporal data collection over arbitrary data sources

## Installation

Install the library from source:

```
python -m venv venv-collekt
. venv-collect/bin/activate

git clone https://github.com/ai4copsec/collekt.git
pip install ./collekt
```

## Usage
We are starting to develop the library and interface, so frequent changes might be possible.
However, the following examples show how the commandline interface can be used:

```
$> collekt query --from-time 2026-04-06 --to-time 2026-04-06 --at-lat 50 --at-lon 5 --radius 50 --output-dir all-results
```

If a datasource requires a login, add this information to the .env file.
The following credentials are requires per datasource


To limit the datasource use the '--datasource NAME-OF-DATASOURCE' option:

### DataSource: Skytruth

Connecting and using the data made available by [Skytruth](https://cerulean.skytruth.org).

```
$> collekt query --from-time 2024-01-30 --to-time 2024-01-31 --at-lat 13.311 --at-lon 42.923 --radius 50 --output-dir all-results --datasource skytruth
Subparser: QueryParser
INFO:collekt.core.collector:Starting collection - output_dir=PosixPath('test-spill')
INFO:collekt.core.collector:Querying datasource=<collekt.datasources.skytruth.SkytruthDataset object at 0x72b38ef5b860>
INFO:collekt.datasources.skytruth:Querying skytruth: https://api.cerulean.skytruth.org/collections/public.slick_plus/items?sortby=%2Dslick_timestamp with {'limit': 1000, 'bbox': '42.461524901876295,12.85904799557727,43.38447509812371,13.762935958283682', 'datetime': '2024-01-30T00:00:00Z/2024-01-31T00:00:00Z'}

$> damast inspect -f all-results/skytruth.parquet
...
First 10 and last 10 rows:
shape: (1, 26)
┌─────────┬─────────────────────┬────────────────────┬──────────────────┬─────────────┬───┬───────────────────┬─────────────────────────────────┬───────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ id      ┆ slick_timestamp     ┆ machine_confidence ┆ slick_confidence ┆ length      ┆ … ┆ source_type_2_ids ┆ source_type_3_ids               ┆ max_source_collated_score ┆ slick_url                       ┆ geometry_geojson                │
│ ---     ┆ ---                 ┆ ---                ┆ ---              ┆ ---         ┆   ┆ ---               ┆ ---                             ┆ ---                       ┆ ---                             ┆ ---                             │
│ i64     ┆ str                 ┆ f64                ┆ str              ┆ f64         ┆   ┆ str               ┆ list[str]                       ┆ f64                       ┆ str                             ┆ str                             │
╞═════════╪═════════════════════╪════════════════════╪══════════════════╪═════════════╪═══╪═══════════════════╪═════════════════════════════════╪═══════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ 3494992 ┆ 2024-01-30T15:19:36 ┆ 0.877347           ┆ null             ┆ 5526.727809 ┆ … ┆ null              ┆ ["D33.757328", "D86.53759", "D… ┆ -1.522026                 ┆ https://cerulean.skytruth.org/… ┆ {"type": "MultiPolygon", "coor… │
└─────────┴─────────────────────┴────────────────────┴──────────────────┴─────────────┴───┴───────────────────┴─────────────────────────────────┴───────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
shape: (1, 26)
┌─────────┬─────────────────────┬────────────────────┬──────────────────┬─────────────┬───┬───────────────────┬─────────────────────────────────┬───────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ id      ┆ slick_timestamp     ┆ machine_confidence ┆ slick_confidence ┆ length      ┆ … ┆ source_type_2_ids ┆ source_type_3_ids               ┆ max_source_collated_score ┆ slick_url                       ┆ geometry_geojson                │
│ ---     ┆ ---                 ┆ ---                ┆ ---              ┆ ---         ┆   ┆ ---               ┆ ---                             ┆ ---                       ┆ ---                             ┆ ---                             │
│ i64     ┆ str                 ┆ f64                ┆ str              ┆ f64         ┆   ┆ str               ┆ list[str]                       ┆ f64                       ┆ str                             ┆ str                             │
╞═════════╪═════════════════════╪════════════════════╪══════════════════╪═════════════╪═══╪═══════════════════╪═════════════════════════════════╪═══════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ 3494992 ┆ 2024-01-30T15:19:36 ┆ 0.877347           ┆ null             ┆ 5526.727809 ┆ … ┆ null              ┆ ["D33.757328", "D86.53759", "D… ┆ -1.522026                 ┆ https://cerulean.skytruth.org/… ┆ {"type": "MultiPolygon", "coor… │
└─────────┴─────────────────────┴────────────────────┴──────────────────┴─────────────┴───┴───────────────────┴─────────────────────────────────┴───────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
```

### Datasource: HOZINT API
Set up credentials for the HOZINT api.

```
HOZINT_APICLIENT_USER=your@email.com
HOZINT_APICLIENT_PASSWORD=yourpassword
HOZINT_APICLIENT_CSRF_TOKEN=yourscsrftoken
```

```
$> collekt query --from-time 2024-01-30 --to-time 2024-01-31 --output-dir all-results --datasource hozint
Subparser: QueryParser
INFO:collekt.core.collector:Starting collection - output_dir=PosixPath('all-results')
INFO:collekt.core.collector:Querying datasource=<collekt.datasources.hozint.Hozint object at 0x719e3393df40>
Subparser: QueryParser
[2026-06-19 14:47:45][  INFO  ] hozint_apiclient.core.client: HOZINT Login successful
[2026-06-19 14:47:50][  INFO  ] hozint_apiclient.core.client: Fetching 30 reports (on 1 pages)
[2026-06-19 14:47:50][  INFO  ] hozint_apiclient.core.client:  -- filters={'auto_date_range': 'Custom timeframe', 'event_end_date': '2024-01-31', 'event_start_date': '2024-01-30', 'page': 1, 'page_size': 200}
Page: 0it [00:00, ?it/s]
[2026-06-19 14:47:50][  INFO  ] hozint_apiclient.core.client: Fetching reports: succeeded
[2026-06-19 14:47:50][  INFO  ] hozint_apiclient.cli.query: Saving reports (.parquet) in all-results
Report:: 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 30/30 [00:00<00:00, 2211.52it/s]
[2026-06-19 14:47:50][  INFO  ] hozint_apiclient.core.client: HOZINT Logout successful.
```

$> damast inspect -f all-results/hozint-reports.parquet
```
First 10 and last 10 rows:
shape: (10, 14)
┌───────────┬──────────────┬─────────────────────────────────┬───────────┬─────────────┬───┬──────────────┬─────────────────┬──────────────────┬─────────────────┬──────────────────┐
│ report_id ┆ article_uuid ┆ report_title                    ┆ latitude  ┆ longitude   ┆ … ┆ victims_dead ┆ victims_wounded ┆ victims_hostages ┆ victims_missing ┆ victims_arrested │
│ ---       ┆ ---          ┆ ---                             ┆ ---       ┆ ---         ┆   ┆ ---          ┆ ---             ┆ ---              ┆ ---             ┆ ---              │
│ i64       ┆ str          ┆ str                             ┆ f64       ┆ f64         ┆   ┆ i64          ┆ i64             ┆ i64              ┆ i64             ┆ i64              │
╞═══════════╪══════════════╪═════════════════════════════════╪═══════════╪═════════════╪═══╪══════════════╪═════════════════╪══════════════════╪═════════════════╪══════════════════╡
│ 6612758   ┆ null         ┆ Authorities arrested Daniel N,… ┆ 18.859303 ┆ -99.239367  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6609506   ┆ null         ┆ | Sources in Gaza hospitals: 3… ┆ 31.501695 ┆ 34.466845   ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6609472   ┆ null         ┆ United States U.S. increases i… ┆ 38.794595 ┆ -106.534838 ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6604528   ┆ null         ┆ Teachers’ Recruitment: Applica… ┆ 7.562896  ┆ 4.5199593   ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6604221   ┆ null         ┆ #ASSASSINATION #BIDEN Federal … ┆ 32.318231 ┆ -86.902298  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6604106   ┆ null         ┆ Pingree Grove man found guilty… ┆ 42.068637 ┆ -88.413416  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6603346   ┆ null         ┆ Shooting in Rogoredo, now the … ┆ 45.676032 ┆ 9.3174993   ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6601447   ┆ null         ┆ Arrested for child pornography… ┆ 41.157944 ┆ -8.629105   ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6600434   ┆ null         ┆ CORRECTED-UPDATE 1-Minnesota m… ┆ 47.439654 ┆ -92.921021  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 6600113   ┆ null         ┆ GOP wants Biss to testify in C… ┆ 42.056459 ┆ -87.675267  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
└───────────┴──────────────┴─────────────────────────────────┴───────────┴─────────────┴───┴──────────────┴─────────────────┴──────────────────┴─────────────────┴──────────────────┘
shape: (10, 14)
┌───────────┬──────────────┬─────────────────────────────────┬───────────┬────────────┬───┬──────────────┬─────────────────┬──────────────────┬─────────────────┬──────────────────┐
│ report_id ┆ article_uuid ┆ report_title                    ┆ latitude  ┆ longitude  ┆ … ┆ victims_dead ┆ victims_wounded ┆ victims_hostages ┆ victims_missing ┆ victims_arrested │
│ ---       ┆ ---          ┆ ---                             ┆ ---       ┆ ---        ┆   ┆ ---          ┆ ---             ┆ ---              ┆ ---             ┆ ---              │
│ i64       ┆ str          ┆ str                             ┆ f64       ┆ f64        ┆   ┆ i64          ┆ i64             ┆ i64              ┆ i64             ┆ i64              │
╞═══════════╪══════════════╪═════════════════════════════════╪═══════════╪════════════╪═══╪══════════════╪═════════════════╪══════════════════╪═════════════════╪══════════════════╡
│ 4093490   ┆ null         ┆ Pilot Killed in Small Plane Cr… ┆ 35.759573 ┆ -79.0193   ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3810611   ┆ null         ┆ @Mlombardo2 @JustLuai Salwan M… ┆ 59.332704 ┆ 18.065625  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3667123   ┆ null         ┆ @zuhaib_sheikh1 @IndiaTales7 I… ┆ 39.952584 ┆ -75.165222 ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3666288   ┆ null         ┆ @Jyotipraka29693 @zoo_bear The… ┆ 39.952584 ┆ -75.165222 ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3570548   ┆ null         ┆ Uganda declares end of Ebola o… ┆ 0.3151692 ┆ 32.581631  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3564956   ┆ null         ┆ Uganda Declares End of Sudan V… ┆ 0.3151692 ┆ 32.581631  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3556616   ┆ null         ┆ FBI arrests judge in escalatio… ┆ 38.892287 ┆ -77.005479 ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3468534   ┆ null         ┆ Clashes hit rebel-controlled G… ┆ -1.658501 ┆ 29.220455  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3304516   ┆ null         ┆ Turkish Court Orders Erdogan P… ┆ 41.069085 ┆ 28.980297  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
│ 3075283   ┆ null         ┆ Bhubaneswar Police Dismantle M… ┆ 20.237199 ┆ 85.834128  ┆ … ┆ 0            ┆ 0               ┆ 0                ┆ 0               ┆ 0                │
└───────────┴──────────────┴─────────────────────────────────┴───────────┴────────────┴───┴──────────────┴─────────────────┴──────────────────┴─────────────────┴──────────────────┘
```

### Datasource: copernicusmarine
```
COPERNICUSMARINE_SERVICE_USERNAME=your@email.com
COPERNICUSMARINE_SERVICE_PASSWORD=yourcmspassword
```

## Testing

Install the project and use the predefined default test environment:

    pytest tests

## Contributing

This project is open to contributions. For details on how to contribute please check the [Contribution Guidelines](https://github.com/ai4copsec/collekt/blob/main/CONTRIBUTING.md)


## License
This project is licensed under the [BSD-3-Clause License](https://github.com/ai4copsec/collekt/blob/main/LICENSE).

## Copyright

Copyright (c) 2026 [Simula Research Laboratory, Oslo, Norway](https://www.simula.no/research/research-departments)

## Acknowledgments

The development of this library is part of the EU-project [AI4COPSEC](https://ai4copsec.eu) which receives funding from the Horizon Europe framework programme under Grant Agreement N. 101190021.
