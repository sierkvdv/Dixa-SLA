## Power BI ingest instructions

### Folder ingest (daily files)

Power Query (M):

```
let
    Source = Folder.Files("C:\\pad\\naar\\repo\\data\\dixa_daily"),
    KeepCsv = Table.SelectRows(Source, each Text.EndsWith([Name], ".csv")),
    Imported = Table.AddColumn(KeepCsv, "Data", each Csv.Document(File.Contents([Folder Path]&[Name]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv])),
    Expand = Table.ExpandTableColumn(Imported, "Data", null, null),
    Promote = Table.PromoteHeaders(Expand, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promote, {
        {"createdAt", type datetime}, {"answeredAt", type datetime}, {"closedAt", type datetime}
    })
in
    Typed
```

### Calendar (DAX)

```
Calendar =
VAR MinDate = DATE(2020,1,1)
VAR MaxDate = TODAY() + 1
RETURN CALENDAR(MinDate, MaxDate)

Year = YEAR('Calendar'[Date])
MonthNum = MONTH('Calendar'[Date])
Month = FORMAT('Calendar'[Date], "YYYY-MM")
```

Mark as date table → 'Calendar'[Date].

Relationship: 'Calendar'[Date] → Fact[createdAt] (1:*).

Slicer on 'Calendar'[Date] with Between. X-axis visuals → Continuous.

### Incremental Refresh (optional)

Parameters RangeStart/RangeEnd (Date/Time).

Make filter foldable:

```
= Table.SelectRows(Source, each [createdAt] >= RangeStart and [createdAt] < RangeEnd)
```

Store last 36 months, Refresh last 7 days (example).

### Notes

- `export_dixa_refresh.py` supports:
  - `--range START END` or env `DIXA_START_ISO` / `DIXA_END_ISO`
  - `--daily-files` to write per-day CSVs into `./data/dixa_daily/`
  - `--single-file` to write one `conversations_ytd.csv`
  - `--channel ""` to fetch all channels (no channel filter)
- `export_dixa_prev_month_exports.py` exports previous month only and is not suitable for full-history Power BI.

### Quick run

```
# alle kanalen in week:
DIXA_TOKEN=... DIXA_USE_BEARER=true \
python export_dixa_refresh.py --range 2025-06-01 2025-06-08 --single-file --channel ""

# per dag:
python export_dixa_refresh.py --range 2025-06-01 2025-06-08 --daily-files --channel ""
```

### Power BI (Folder.Files)

Gebruik Folder.Files op data/dixa_daily.

DAX Calendar (minimal):

```
Calendar = CALENDAR(DATE(2020,1,1), TODAY()+1)
```

Mark as Date Table; relate Calendar[Date] -> Fact[createdAt]; slicer op Calendar[Date] (Between).

### Config en run (kort)

- Maak een `.env` in de projectroot met minimaal:
  - `DIXA_TOKEN=...`
  - Optioneel: `DIXA_USE_BEARER=true`, `DIXA_BASE_URL`, `DIXA_EXPORTS_BASE`

Run:

```
python export_dixa_refresh.py --range 2025-06-01 2025-06-03 --single-file
python export_dixa_refresh.py --range 2025-06-01 2025-06-03 --daily-files
```

Power BI: gebruik Folder.Files op `data/dixa_daily`. Calendar: `Calendar = CALENDAR(DATE(2020,1,1), TODAY()+1)`. Mark as Date Table.

### Smoke test / Expected results

Run:

```
python export_dixa_refresh.py --range 2025-01-01 2025-01-03 --single-file --channel ""
python export_dixa_refresh.py --range 2025-01-01 2025-01-03 --daily-files --channel ""
```

Expected:

- conversations_ytd.csv exists (single-file run)
- data/dixa_daily/conversations_2025-01-01.csv, ..._2025-01-02.csv, ..._2025-01-03.csv exist (daily-files run)
- Console shows HTTP 200; payload is printed; "Empty results" message appears only when there is truly no data

