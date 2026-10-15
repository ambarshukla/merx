# Glossary

Every acronym is expanded here. Terms are added in the same change that first
uses them.

| Term | Meaning |
|---|---|
| **Asset (Dagster)** | A dataset that Dagster knows how to build and tracks: here, a raw file (`raw/<id>`) or a staged table (`staged/<id>`). |
| **Asset check (Dagster)** | A test attached to an asset, recorded with its result and evidence on every run. Each check in a contract becomes one. |
| **Backwardation** | A forward curve where later delivery is cheaper than nearer delivery, usually a sign of tight supply. |
| **Bitemporal** | Storing two times per fact: when it applied (valid time) and when we learned it (knowledge time). |
| **Blocking check** | An asset check whose failure stops anything downstream in the same run. Here: error-severity checks on tier 1 datasets. |
| **Brent** | The main international crude oil price benchmark, originally for oil from the North Sea. |
| **CFTC (Commodity Futures Trading Commission)** | The US regulator of futures markets. Publishes weekly reports of who holds futures positions. |
| **Contango** | A forward curve where later delivery costs more than nearer delivery, usually a sign of ample supply and storage. |
| **Contract 1 (front month)** | The futures contract nearest to expiry. "Contract 1" is a series that rolls to the next contract as each one expires. |
| **Crosswalk** | A table that maps each provider's identifier for something onto our own identifier, so data is joined on codes, not on names that can change. |
| **Dagster** | An open-source orchestrator: it runs pipeline steps in order, on schedules, and records every run and check. |
| **Data contract** | A declarative description of a dataset: source, schema, units, calendar, tier, release schedule, owner and rules. |
| **DuckDB** | An analytical SQL database that runs inside the program using it and stores everything in one file. Allows one writing process per file. |
| **EIA (U.S. Energy Information Administration)** | The statistics agency of the US Department of Energy. Publishes energy prices, stocks and production data. |
| **FOB (free on board)** | A price for a cargo at the point of loading, before shipping costs. |
| **Freshness check** | Fails when the newest value in a dataset is older than a stated number of days. Looks at the data, not at the file's date. |
| **Futures contract** | An exchange-traded agreement to buy or sell a set quantity of a commodity at a set price on a future date. |
| **Henry Hub** | A natural gas pipeline hub in Erath, Louisiana; its price is the main US natural gas benchmark. |
| **Idempotent** | Running a step twice gives the same result as running it once. Staging is idempotent: it rebuilds the table from one raw file. |
| **Knowledge time** | The moment a value became known to us. Used to rebuild history "as known then". |
| **MAD (median absolute deviation)** | A robust measure of spread: the median of the distances from the median. Unlike standard deviation, one spike does not distort it. |
| **Manifest** | The table recording every raw download: where it came from, when, its hash and size. |
| **MMBtu** | One million British thermal units, the standard unit for pricing natural gas in the US. |
| **NYMEX (New York Mercantile Exchange)** | The exchange where WTI crude oil and Henry Hub natural gas futures trade. |
| **PRA (price reporting agency)** | An organisation that assesses physical commodity prices from market evidence, for example Platts or Argus. |
| **Raw zone (landing zone)** | Where downloads are stored exactly as received, never edited. Everything downstream is rebuilt from it. |
| **Release calendar** | When a source publishes each update. EIA states the current and next release date inside each file. |
| **Resource (Dagster)** | A shared service an asset asks for by name, such as the downloader or the data store; tests swap in a fake. |
| **Roll** | Moving from an expiring futures contract to the next one. A "generic" series such as front-month rolls by a stated rule. |
| **Settlement price** | The exchange's official end-of-day price, used for margining. Distinct from the last traded price. |
| **SHA-256** | A fingerprint of a file's bytes. Identical files share it; any change gives a different one. |
| **Spot price** | The price for a commodity delivered now (within days), as opposed to a futures price for later delivery. |
| **SPR (Strategic Petroleum Reserve)** | The US government's emergency crude oil stockpile, reported separately from commercial stocks. |
| **Staged table** | The typed, checked version of one dataset (`staged.<id>` in DuckDB), with each row linked to the raw file it came from. |
| **Tier** | How critical a dataset is. Tier 1 gets blocking checks and immediate alerts; lower tiers get warnings and next-day review. |
| **WTI (West Texas Intermediate)** | The main US crude oil price benchmark, priced at Cushing, Oklahoma. |
