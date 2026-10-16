"use strict";
// Plain-English explanations of the words this site uses.
//
// Any element marked data-explain="key" gets a small marker; hovering, focusing or
// tapping it shows the explanation. A page with a #concepts element also gets the
// whole list rendered into it, so the terms can be read straight through rather than
// hunted for.
//
// The wording assumes no knowledge of commodities or data engineering, which is the
// point: a data product that cannot explain itself is only usable by the people who
// built it.

const EXPLAIN = {
  "sector": {
    title: "Sector",
    short: "The family a commodity belongs to.",
    long: `Oil, gas, power (electricity), carbon, metals and agriculture. They trade in
      different places, in different units, on different clocks — electricity is priced
      every half hour, a crop price might move once a month — which is why a platform
      that handles all six has to be built for the differences rather than around them.`,
  },
  "data-type": {
    title: "Type of data",
    short: "What kind of fact a number is.",
    long: `A <b>spot</b> price is what the thing costs now. A <b>fundamental</b> is a
      physical quantity behind the price — barrels in storage, gas in the ground.
      <b>Positioning</b> is who holds what in the futures market. <b>Environmental</b>
      covers emissions and intensity. The same commodity usually has several: WTI crude
      has a spot price, a stock level and a positioning report.`,
  },
  "spot-price": {
    title: "Spot price",
    short: "The price for delivery now, rather than later.",
    long: `As opposed to a futures price, which is a price agreed today for delivery on
      a set date in the future. "Now" in practice means within a few days.`,
  },
  "benchmark": {
    title: "Benchmark",
    short: "A reference price the market quotes other deals against.",
    long: `Nobody publishes the price of every cargo of oil. Instead a few well-defined
      prices — WTI at Cushing in Oklahoma, Brent in the North Sea, Henry Hub for US gas
      — act as reference points, and real contracts are written as "the benchmark plus
      or minus a difference". That is why a handful of series matter far more than their
      trading volume suggests.`,
  },
  "tier": {
    title: "Tier",
    short: "How critical a dataset is, which decides how strictly it is policed.",
    long: `Tier 1 is critical: if an important check fails, the new data is not published
      at all and users keep seeing the last good version. Lower tiers are published with
      a warning instead. Tiering is how you avoid either blocking everything or checking
      nothing.`,
  },
  "publish-gate": {
    title: "Publish gate",
    short: "The step that decides whether new data reaches users.",
    long: `Every build runs its checks first. The gate reads the results and either
      publishes the data or holds it back. The important part is what happens when it
      holds back: users keep the last version that passed, and are told newer data
      exists but is being withheld — which is almost always better than quietly serving
      something wrong.`,
  },
  "check": {
    title: "Check",
    short: "A rule the data has to satisfy before anyone sees it.",
    long: `Declared in the dataset's contract, not buried in code: no missing values, no
      duplicate dates, values inside a plausible range, the newest value recent enough,
      the parts adding up to the total. A failing check is not just a log line — it
      becomes an exception with an owner.`,
  },
  "exception": {
    title: "Exception",
    short: "A failed check turned into a piece of work with someone's name on it.",
    long: `It records when the problem first appeared, how many times it has been seen,
      who owns it, and — once a person has looked — the <b>root cause</b>: was the source
      late, did the file layout change, did the source publish something wrong, was it a
      real market event, or was our own rule too strict? Those categories are what let
      you say later which kind of problem wastes the most time.`,
  },
  "freshness": {
    title: "Freshness",
    short: "Whether the newest value is recent enough to be useful.",
    long: `Measured on the data, not on the file. A source can republish an unchanged
      file every day: the download looks healthy while the series has quietly stopped
      moving. Freshness looks at the newest actual value instead.`,
  },
  "as-known-on": {
    title: "As known on (point-in-time)",
    short: "The data as it looked on a past date, not as it looks now.",
    long: `Sources revise history. If you test an idea using today's corrected numbers,
      you are using information nobody had at the time, and your results will look better
      than reality — that mistake is called <b>look-ahead bias</b>. merx keeps every
      version of every value with the time it became known, so any past date can be
      rebuilt exactly.`,
  },
  "revision": {
    title: "Revision",
    short: "A value the source changed after first publishing it.",
    long: `Common and usually legitimate: an estimate is replaced by a measurement. What
      matters is that it is recorded rather than overwritten, so you can see that a
      number moved, when, and by how much.`,
  },
  "withdrawal": {
    title: "Withdrawal",
    short: "A value that was there before and has vanished.",
    long: `Harder to notice than a wrong value, because checks that look at the rows
      present cannot see rows that are gone. merx compares each build against everything
      it has ever known. This is not hypothetical: three oil and gas series here lost
      thousands of historical prices from their source files in 2026.`,
  },
  "reconciliation": {
    title: "Reconciliation",
    short: "Comparing two sources for the same number, value by value.",
    long: `Two kinds, and the difference matters. The same publisher through two
      <b>channels</b> — a spreadsheet and an API — should agree exactly, so any
      difference is a fault. Two different <b>institutions</b> computing the same thing
      will not agree exactly, because their methods differ; there the job is to know the
      size of the gap and be able to explain it.`,
  },
  "settlement-period": {
    title: "Settlement period",
    short: "The half hour that Great Britain's electricity market settles in.",
    long: `Numbered 1 to 48 within each day. Two days a year the clocks change and the
      local day is 23 or 25 hours long, so it has 46 or 50 periods instead — which is
      exactly why the market counts periods rather than using timestamps, and why merx
      does too.`,
  },
  "negative-price": {
    title: "Negative price",
    short: "Being paid to take something away.",
    long: `It sounds like a data error and usually is not. In electricity, when wind and
      solar are producing hard and demand is low, it can be cheaper to pay someone to
      consume power than to switch generation off and on again. Oil did it too: US crude
      settled below zero in April 2020 when there was nowhere left to store it. A system
      that "corrects" negative prices destroys real data.`,
  },
  "carbon-intensity": {
    title: "Carbon intensity",
    short: "How much CO2 came with each unit of electricity.",
    long: `Measured in grams of CO2 per kilowatt hour. It swings through the day with the
      generation mix: low when wind and solar are strong, high when gas plants are
      filling the gap. The GB grid publishes both a <b>forecast</b> and the
      <b>outturn</b> — what actually happened — for every half hour.`,
  },
  "forecast-error": {
    title: "Forecast error",
    short: "How far a prediction turned out to be from reality.",
    long: `Because this source publishes the forecast and the outturn for the same half
      hour, accuracy can simply be measured rather than argued about. Two numbers are
      usually quoted: the average error, which shows whether a forecast leans high or
      low, and the average <i>absolute</i> error, which shows how far off it typically is
      in either direction. A forecast can have almost no average error and still be
      wildly wrong every single time.`,
  },
  "measure": {
    title: "Measure",
    short: "One of the numbers a dataset carries.",
    long: `A price series has one. A positioning report has many — long positions, short
      positions, open interest — all sharing a date. Storing them as one row per measure
      per date, rather than one column each, is what lets one engine handle every
      dataset.`,
  },
  "unit": {
    title: "Unit",
    short: "What a number is counted in.",
    long: `Dollars per barrel, dollars per tonne, pounds per megawatt hour, grams of CO2
      per kilowatt hour. Units are not decoration: the same commodity quoted per tonne
      instead of per kilogram differs by a factor of a thousand, and the number still
      looks perfectly reasonable. Where a source states its own units, merx checks them
      against what the contract expects.`,
  },
  "contract": {
    title: "Data contract",
    short: "A file describing a dataset, which the engine then runs.",
    long: `Where it comes from, what it measures, in what units, how often it arrives,
      how critical it is, and the rules it must pass. There is one engine; adding a
      dataset means adding a contract, not writing a pipeline. That is the whole design
      in one sentence.`,
  },
};

// Attaches the "?" markers. Safe to call again after part of the page is rebuilt:
// elements that already carry a marker are left alone.
let attachMarkers = function () {};

function explainPopover() {
  const box = document.createElement("div");
  box.className = "explain-pop";
  box.hidden = true;
  document.body.appendChild(box);
  let openFor = null;

  function show(anchor, key) {
    const term = EXPLAIN[key];
    if (!term) return;
    box.innerHTML = `<b>${term.title}</b><p>${term.long}</p>`;
    box.hidden = false;
    const r = anchor.getBoundingClientRect();
    const width = Math.min(360, window.innerWidth - 24);
    box.style.width = width + "px";
    let left = r.left + window.scrollX;
    left = Math.min(left, window.scrollX + window.innerWidth - width - 12);
    box.style.left = Math.max(window.scrollX + 12, left) + "px";
    box.style.top = r.bottom + window.scrollY + 6 + "px";
    openFor = anchor;
  }

  function hide() {
    box.hidden = true;
    openFor = null;
  }

  attachMarkers = function () {
    document.querySelectorAll("[data-explain]").forEach((el) => {
      const key = el.dataset.explain;
      const term = EXPLAIN[key];
      if (!term || el.classList.contains("explained")) return;
      el.classList.add("explained");
      const mark = document.createElement("button");
      mark.className = "explain-mark";
      mark.type = "button";
      mark.textContent = "?";
      mark.setAttribute("aria-label", `What is ${term.title}?`);
      mark.title = term.short;
      el.appendChild(mark);
      mark.addEventListener("mouseenter", () => show(mark, key));
      mark.addEventListener("focus", () => show(mark, key));
      mark.addEventListener("mouseleave", () => {
        if (!box.matches(":hover")) hide();
      });
      mark.addEventListener("blur", hide);
      mark.addEventListener("click", (e) => {
        e.preventDefault();
        if (openFor === mark) hide();
        else show(mark, key);
      });
    });
  };

  attachMarkers();
  box.addEventListener("mouseleave", hide);
}

// Pages that rebuild part of themselves call this to mark the new content.
function explainAgain() {
  attachMarkers();
}

// Renders every concept as an expandable list, in the order given.
function renderConcepts(target, keys) {
  const el = typeof target === "string" ? document.getElementById(target) : target;
  if (!el) return;
  el.innerHTML = keys
    .filter((k) => EXPLAIN[k])
    .map(
      (k) => `<details class="concept"><summary><b>${EXPLAIN[k].title}</b>
        <span class="muted">— ${EXPLAIN[k].short}</span></summary>
        <p>${EXPLAIN[k].long}</p></details>`
    )
    .join("");
}

document.addEventListener("DOMContentLoaded", explainPopover);
