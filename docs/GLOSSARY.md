# Glossary

Every acronym is expanded here. Terms are added in the same change that first
uses them.

| Term | Meaning |
|---|---|
| **As known at** | A dataset rebuilt exactly as it stood at a past moment, using only the versions known then. |
| **Accepted withdrawal** | A value known before and missing now whose removal a person has signed off, with a reason. It no longer fails the withdrawal check. |
| **Asset (Dagster)** | A dataset that Dagster knows how to build and tracks: here, a raw file (`raw/<id>`) or a staged table (`staged/<id>`). |
| **Asset check (Dagster)** | A test attached to an asset, recorded with its result and evidence on every run. Each check in a contract becomes one. |
| **Backwardation** | A forward curve where later delivery is cheaper than nearer delivery, usually a sign of tight supply. |
| **Balance check** | A check that an accounting identity holds on every date, e.g. open interest equals all long positions plus spreads. |
| **Bcf** | Billion cubic feet, the unit for US natural gas volumes such as gas in storage. |
| **Bitemporal** | Storing two times per fact: when it applied (valid time) and when we learned it (knowledge time). |
| **Blocking check** | An asset check whose failure stops anything downstream in the same run. Here: error-severity checks on tier 1 datasets. |
| **Build (staged build)** | One rebuild of a staged table, identified by a fingerprint of the raw files it came from. Check results and publications refer to it. |
| **Break (reconciliation)** | A disagreement between two channels for the same value: different values, or a date only one of them has. |
| **Brent** | The main international crude oil price benchmark, originally for oil from the North Sea. |
| **Carried-forward value** | A value repeated from the previous date to fill a day with no real observation, such as a market holiday. |
| **CFTC (Commodity Futures Trading Commission)** | The US regulator of futures markets. Publishes weekly reports of who holds futures positions. |
| **COT (Commitments of Traders)** | The CFTC's weekly report of futures positions by type of trader, as of Tuesday and published the following Friday. |
| **Channel** | One way a source delivers the same data, such as a spreadsheet download or an API. Two channels can disagree. |
| **Client view** | The view for people using the data (`/client`): published series only, with quality status, history and revisions. |
| **Contango** | A forward curve where later delivery costs more than nearer delivery, usually a sign of ample supply and storage. |
| **Contract 1 (front month)** | The futures contract nearest to expiry. "Contract 1" is a series that rolls to the next contract as each one expires. |
| **Crosswalk** | A table that maps each provider's identifier for something onto our own identifier, so data is joined on codes, not on names that can change. |
| **Dagster** | An open-source orchestrator: it runs pipeline steps in order, on schedules, and records every run and check. |
| **Data contract** | A declarative description of a dataset: source, schema, units, calendar, tier, release schedule, owner and rules. |
| **DuckDB** | An analytical SQL database that runs inside the program using it and stores everything in one file. Allows one writing process per file. |
| **EIA (U.S. Energy Information Administration)** | The statistics agency of the US Department of Energy. Publishes energy prices, stocks and production data. |
| **Exception** | A failing check turned into a work item: owner, when it opened and closed, how often it was seen, and its root cause. |
| **FOB (free on board)** | A price for a cargo at the point of loading, before shipping costs. |
| **Freshness check** | Fails when the newest value in a dataset is older than a stated number of days. Looks at the data, not at the file's date. |
| **Futures contract** | An exchange-traded agreement to buy or sell a set quantity of a commodity at a set price on a future date. |
| **Capture time** | When a web archive took its copy of a file. For archived copies it stands in for knowledge time, as an upper bound on when the file was public. |
| **Ground truth** | The record of what is actually wrong, against which detectors are scored. In the lab, the manifest of seeded defects. |
| **Henry Hub** | A natural gas pipeline hub in Erath, Louisiana; its price is the main US natural gas benchmark. |
| **Idempotent** | Running a step twice gives the same result as running it once. Staging is idempotent: it rebuilds the table from one raw file. |
| **Knowledge time** | The moment a value became known to us. Used to rebuild history "as known then". |
| **Long form** | A table shape with one row per (measure, date) and a single value column, rather than one column per measure. |
| **MAD (median absolute deviation)** | A robust measure of spread: the median of the distances from the median. Unlike standard deviation, one spike does not distort it. |
| **Managed money** | In the COT report, funds that trade futures for clients: hedge funds and commodity trading advisers. |
| **Manifest** | The table recording every raw download: where it came from, when, its hash and size. |
| **Measure** | One named number a dataset reports per date, e.g. `price`, or `managed_money_long` in a positioning report. |
| **MMBtu** | One million British thermal units, the standard unit for pricing natural gas in the US. |
| **Non-reportable positions** | Positions of traders too small to be reported individually; the CFTC derives them as the remainder of open interest. |
| **NYMEX (New York Mercantile Exchange)** | The exchange where WTI crude oil and Henry Hub natural gas futures trade. |
| **Open interest** | The number of futures contracts open at a point in time. Each has a long and a short side. |
| **Ops console** | The operations view (`/ops`): dataset status, checks, exceptions, missing values, reconciliation and detector scores. |
| **Partition** | One slice of a dataset that is fetched and tracked on its own, such as one year of a source published as yearly files. |
| **PRA (price reporting agency)** | An organisation that assesses physical commodity prices from market evidence, for example Platts or Argus. |
| **Precision** | Of the values a detector flags, the share that are truly wrong. Low precision means wasted reviewer time. |
| **Producer/merchant** | In the COT report, companies that produce, process or use the physical commodity and use futures to hedge. |
| **Publish gate** | The rule that decides whether a staged build reaches users: every check must have run on it, and for tier 1 no error-level check may fail. |
| **Published layer** | The tables (`published.<id>`) that passed the publish gate: the only data the client view reads. |
| **Quarantine (detector)** | A flagged value is not used as the reference or in the yardstick for judging later values, so one bad value cannot distort the next judgements. |
| **Raw zone (landing zone)** | Where downloads are stored exactly as received, never edited. Everything downstream is rebuilt from it. |
| **Recall** | Of the values that are truly wrong, the share a detector flags. Low recall means errors reach users. |
| **Reconciliation** | Comparing two independent copies of the same data value by value, and explaining every difference. |
| **Release calendar** | When a source publishes each update. EIA states the current and next release date inside each file. |
| **Repeat share** | The share of exceptions that recur on a check that has failed before. A high share means fixes are not holding. |
| **Resource (Dagster)** | A shared service an asset asks for by name, such as the downloader or the data store; tests swap in a fake. |
| **Revision** | A published value replaced by a different one later, for the same date. A whole-history revision is usually a change of definition. |
| **Root cause** | Why an exception happened, from a fixed list (source late, source format change, market event, pipeline bug and so on), assigned by a person. |
| **Roll** | Moving from an expiring futures contract to the next one. A "generic" series such as front-month rolls by a stated rule. |
| **Seeded defect** | A synthetic error deliberately injected into a copy of real data, and recorded, so detectors can be scored exactly. |
| **Settlement price** | The exchange's official end-of-day price, used for margining. Distinct from the last traded price. |
| **SHA-256** | A fingerprint of a file's bytes. Identical files share it; any change gives a different one. |
| **Spot price** | The price for a commodity delivered now (within days), as opposed to a futures price for later delivery. |
| **SPR (Strategic Petroleum Reserve)** | The US government's emergency crude oil stockpile, reported separately from commercial stocks. |
| **Spread position** | Holding both a long and a short futures position (e.g. in different months). Counted on both sides of open interest. |
| **Staged table** | The typed, checked version of one dataset (`staged.<id>` in DuckDB), with each row linked to the raw file it came from. |
| **Stale run (stuck feed)** | The same value repeated where it should have changed, usually because a feed stopped updating. |
| **Swap dealer** | In the COT report, banks and dealers that use futures to hedge swaps they have written for clients. |
| **Tier** | How critical a dataset is. Tier 1 gets blocking checks and immediate alerts; lower tiers get warnings and next-day review. |
| **Time to recover** | How long an exception stayed open, from the first failing check to the first passing one. |
| **Valid date** | The date a value describes, as opposed to when it became known (knowledge time). |
| **Version (point-in-time)** | One value of one (measure, date) during the time it was current, from when it became known to when it was revised or withdrawn. |
| **Wayback Machine** | The Internet Archive's public collection of dated copies of web pages and files. Used here to recover past versions of source files. |
| **Withdrawal** | A value that was in an earlier version of a dataset and is missing from the current one. |
| **WTI (West Texas Intermediate)** | The main US crude oil price benchmark, priced at Cushing, Oklahoma. |
| **Z-score** | How many typical deviations a value lies from normal. The robust version uses the median and MAD instead of the mean and standard deviation. |
