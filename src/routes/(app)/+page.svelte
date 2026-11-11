<script lang="ts">
  import COLORS from "$lib/colors";
  import { iconGlyph } from "$lib/icon";
  import {
    nextUnpaidSchedule,
    totalRecurring,
    enrichTrantionSequence,
    sortTrantionSequence
  } from "$lib/transaction_sequence";
  import {
    ajax,
    formatCurrency,
    formatCurrencyCrude,
    formatFloat,
    formatPercentage,
    now,
    type AllocationTarget,
    type CashFlow,
    type GoalSummary,
    type Issue,
    type Networth,
    type Posting,
    type Transaction,
    type TransactionSequence
  } from "$lib/utils";
  import dayjs from "dayjs";
  import _ from "lodash";
  import { onMount } from "svelte";
  import ZeroState from "$lib/components/ZeroState.svelte";
  import { refresh } from "../../store";

  const month = now().format("YYYY-MM");
  const PERIODS = [
    { key: "1M", months: 1 },
    { key: "3M", months: 3 },
    { key: "1Y", months: 12 },
    { key: "All", months: 0 }
  ];

  let isEmpty = false;
  let loaded = false;
  let period = "1Y";
  let networth: Networth;
  let xirr = 0;
  let timeline: Networth[] = [];
  let allocation: AllocationTarget[] = [];
  let issues: Issue[] = [];
  let cashFlows: CashFlow[] = [];
  let expenses: Record<string, Posting[]> = {};
  let goals: GoalSummary[] = [];
  let sequences: TransactionSequence[] = [];
  let transactions: Transaction[] = [];

  async function initDemo() {
    await ajax("/api/init", { method: "POST" });
    refresh();
  }

  onMount(async () => {
    const [dashboard, nw, alloc, diagnosis] = await Promise.all([
      ajax("/api/dashboard"),
      ajax("/api/networth"),
      ajax("/api/allocation").catch(() => null),
      ajax("/api/diagnosis").catch(() => null)
    ]);

    networth = dashboard.networth.networth;
    xirr = dashboard.networth.xirr;
    cashFlows = dashboard.cashFlows;
    expenses = dashboard.expenses;
    goals = _.sortBy(dashboard.goalSummaries, (g) => -g.priority);
    sequences = sortTrantionSequence(enrichTrantionSequence(dashboard.transactionSequences));
    transactions = dashboard.transactions;
    timeline = nw.networthTimeline || [];
    allocation = alloc?.allocation_targets || [];
    issues = diagnosis?.issues || [];
    isEmpty = _.isEmpty(transactions);
    loaded = true;
  });

  // ---- net worth over the selected period -------------------------------
  $: windowed = periodPoints(timeline, period);
  $: first = _.first(windowed);
  $: last = _.last(windowed);
  $: change = first && last ? last.balanceAmount - first.balanceAmount : 0;
  $: added = first && last ? last.netInvestmentAmount - first.netInvestmentAmount : 0;
  $: marketGain = change - added;
  $: changePct = first && first.balanceAmount > 0 ? (change / first.balanceAmount) * 100 : null;
  $: chart = sparkline(windowed);
  $: gainPercent =
    networth && networth.netInvestmentAmount > 0
      ? (networth.gainAmount / networth.netInvestmentAmount) * 100
      : null;

  function periodPoints(points: Networth[], key: string) {
    const months = _.find(PERIODS, (p) => p.key == key).months;
    if (months == 0) return points;
    const cutoff = now().subtract(months, "month");
    const w = _.filter(points, (p) => !p.date.isBefore(cutoff));
    return w.length > 1 ? w : points;
  }

  function sparkline(points: Networth[]) {
    if (points.length < 2) return null;
    const step = Math.max(1, Math.floor(points.length / 240));
    const pts = _.filter(points, (_p, i) => i % step == 0 || i == points.length - 1);
    const values = _.flatMap(pts, (p) => [p.balanceAmount, p.netInvestmentAmount]);
    const lo = _.min(values);
    const hi = _.max(values);
    const span = hi - lo || 1;
    const W = 600;
    const H = 120;
    const x = (i: number) => (i / (pts.length - 1)) * W;
    const y = (v: number) => H - 6 - ((v - lo) / span) * (H - 12);
    const line = (f: (p: Networth) => number) =>
      _.map(pts, (p, i) => `${i == 0 ? "M" : "L"}${x(i).toFixed(1)},${y(f(p)).toFixed(1)}`).join(
        " "
      );
    const worth = line((p) => p.balanceAmount);
    return {
      worth,
      invested: line((p) => p.netInvestmentAmount),
      area: `${worth} L${W},${H} L0,${H} Z`
    };
  }

  // Show the targets that are furthest off first; keep the card a fixed, compact size
  $: allocationRows = _.take(
    _.orderBy(allocation, (t) => Math.abs(t.current - t.target), "desc"),
    6
  );
  $: allocMax = _.max(_.flatMap(allocationRows, (t) => [t.current, t.target])) || 1;

  // ---- this month ---------------------------------------------------------
  $: monthExpenses = expenses[month] || [];
  $: spent = _.sumBy(monthExpenses, (p) => p.amount);
  $: thisMonth = _.find(cashFlows, (c) => c.date.format("YYYY-MM") == month) || _.last(cashFlows);
  $: income = thisMonth ? thisMonth.income : 0;
  $: savingsRate =
    income > 0
      ? ((income - Math.abs(thisMonth.expenses) - Math.abs(thisMonth.tax)) / income) * 100
      : null;
  $: topCategories = _.chain(monthExpenses)
    .groupBy((p) => p.account.split(":")[1] || p.account)
    .map((ps, name) => ({ name, actual: _.sumBy(ps, (p) => p.amount) }))
    .orderBy((r) => r.actual, "desc")
    .take(6)
    .value();
  $: spendRows = topCategories;
  $: maxSpend = _.max(_.map(spendRows, (r) => r.actual)) || 1;
  $: flowMax = _.max(_.flatMap(cashFlows, (c) => [c.income, Math.abs(c.expenses)])) || 1;

  function restName(account: string) {
    return _.drop(account.split(":")).join(":");
  }

  // ---- things that need attention ---------------------------------------
  interface Alert {
    tone: "danger" | "warning" | "info";
    text: string;
    href: string;
  }

  $: alerts = buildAlerts(sequences, allocation, issues);

  function buildAlerts(
    seqs: TransactionSequence[],
    targets: AllocationTarget[],
    problems: Issue[]
  ): Alert[] {
    const out: Alert[] = [];
    _.each(seqs, (ts) => {
      const next = nextUnpaidSchedule(ts);
      if (next && next.scheduled.isBefore(now())) {
        out.push({
          tone: "danger",
          text: `${ts.key} (${formatCurrencyCrude(
            totalRecurring(ts)
          )}) was due ${next.scheduled.fromNow()}`,
          href: "/cash_flow/recurring"
        });
      }
    });
    const off = _.filter(targets, (t) => Math.abs(t.current - t.target) >= 5);
    if (off.length > 0) {
      out.push({
        tone: "info",
        text: `${off.length} allocation target${
          off.length > 1 ? "s are" : " is"
        } 5 or more points off`,
        href: "/assets/allocation"
      });
    }
    const serious = _.filter(problems, (i) => i.level == "danger" || i.level == "warning");
    if (serious.length > 0) {
      out.push({
        tone: "warning",
        text: `${serious.length} issue${serious.length > 1 ? "s" : ""} found in your ledger data`,
        href: "/more/doctor"
      });
    }
    return _.sortBy(out, (a) => ({ danger: 0, warning: 1, info: 2 })[a.tone]);
  }

  // ---- bills, goals, activity ---------------------------------------------
  $: upcoming = _.chain(sequences)
    .map((ts) => ({ ts, next: nextUnpaidSchedule(ts) }))
    .filter((u) => !!u.next)
    .sortBy((u) => u.next.scheduled.valueOf())
    .take(5)
    .value();

  function goalPercent(g: GoalSummary) {
    return g.target > 0 ? Math.min(100, (g.current / g.target) * 100) : 0;
  }

  function goalEta(g: GoalSummary) {
    const d = dayjs(g.targetDate, "YYYY-MM-DD", true);
    return d.isValid() ? d.fromNow() : "";
  }

  function activity(t: Transaction) {
    const amount = _.max(_.map(t.postings, (p) => Math.abs(p.amount))) || 0;
    const accounts = _.uniq(_.map(t.postings, (p) => _.last(p.account.split(":")))).join(" · ");
    return { amount, accounts };
  }
</script>

<section class="section" class:is-hidden={!isEmpty}>
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12">
        <ZeroState item={!isEmpty}>
          <div class="has-text-left" style="max-width: 640px;">
            <p class="mb-2">
              Looks like you are new here, you can either get started or look at a demo setup
            </p>
            <div>
              <p class="is-size-4">I want to get started</p>
              <ol class="ml-5 mt-2 mb-4">
                <li>
                  Go to <a href="/more/config">configuration</a> page and set your default currency and
                  locale.
                </li>
                <li>
                  Go to <a href="/ledger/editor">editor</a> page and start adding transactions to your
                  journal.
                </li>
              </ol>
              <p class="is-size-4">I want to view a Demo</p>
              <p class="ml-3"></p>
              <ol class="ml-5 mt-2 mb-4">
                <li>
                  Click the button below to load a demo setup. This will load a demo journal with
                  relevant config.
                </li>
                <li>
                  Once you are done playing around, you can go to <a href="/ledger/editor">editor</a
                  > page and select all the content and delete them.
                </li>
                <li>
                  Go to <a href="/more/config">configuration</a> page and click the reset to defaults
                  button.
                </li>
              </ol>

              <a on:click={(_e) => initDemo()} class="button is-link">Setup Demo</a>
            </div>
          </div>
        </ZeroState>
      </div>
    </div>
  </div>
</section>

<section class="dash" class:is-hidden={isEmpty || !loaded}>
  <header class="d-page-head">
    <span class="d-muted">As of {now().format("DD MMM YYYY")}</span>
  </header>

  <!-- 1. Am I getting richer? -->
  <div class="d-hero-row">
    <a class="d-hero" href="/assets/networth">
      <div class="d-hero-top">
        <div>
          <span class="stat-label">Net worth</span>
          <span class="stat-value is-lg"
            >{networth ? formatCurrency(networth.balanceAmount) : ""}</span
          >
          <span class="d-hero-change" class:neg={change < 0}>
            <i class="fas {change >= 0 ? 'fa-arrow-trend-up' : 'fa-arrow-trend-down'}" />
            {change >= 0 ? "+" : ""}{formatCurrencyCrude(change)}
            {#if changePct !== null}({formatFloat(changePct, 1)}%){/if}
            <span class="d-hero-note">in {period == "All" ? "total" : period}</span>
          </span>
        </div>
        {#if networth}
          <div class="d-hero-stats">
            <div>
              <span class="stat-label">Absolute return</span>
              <b class="stat-value" class:is-neg={networth.gainAmount < 0}>
                {networth.gainAmount >= 0 ? "+" : ""}{formatCurrencyCrude(networth.gainAmount)}
                {#if gainPercent !== null}
                  <small>({gainPercent >= 0 ? "+" : ""}{formatFloat(gainPercent, 1)}%)</small>
                {/if}
              </b>
            </div>
            <div>
              <span class="stat-label">XIRR (per year)</span>
              <b class="stat-value" class:is-neg={xirr < 0}>{formatFloat(xirr)}%</b>
            </div>
          </div>
        {/if}
      </div>

      {#if chart}
        <svg class="d-spark" viewBox="0 0 600 100" preserveAspectRatio="none">
          <defs>
            <linearGradient id="sparkfill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0" stop-color="#fff" stop-opacity="0.28" />
              <stop offset="1" stop-color="#fff" stop-opacity="0" />
            </linearGradient>
          </defs>
          <path d={chart.area} fill="url(#sparkfill)" />
          <path d={chart.invested} class="invested" />
          <path d={chart.worth} class="worth" />
        </svg>
      {/if}

      <div class="d-hero-foot">
        <span><b>{formatCurrencyCrude(added)}</b> you added</span>
        <span>
          <b>{marketGain >= 0 ? "+" : ""}{formatCurrencyCrude(marketGain)}</b> market movement
        </span>
        <div class="d-tabs" on:click|preventDefault|stopPropagation>
          {#each PERIODS as p}
            <button
              class:active={period == p.key}
              on:click|preventDefault|stopPropagation={() => (period = p.key)}>{p.key}</button
            >
          {/each}
        </div>
      </div>
    </a>

    <div class="d-card">
      <div class="d-card-head">
        <a href="/assets/allocation">Asset allocation</a>
        {#if allocation.length > 6}
          <a class="d-muted" href="/assets/allocation">All {allocation.length}</a>
        {/if}
      </div>
      {#if !_.isEmpty(allocation)}
        <div class="d-alloc-list">
          {#each allocationRows as t}
            <div class="d-alloc" class:off={Math.abs(t.current - t.target) >= 5}>
              <span class="d-alloc-name truncate">{t.name}</span>
              <div class="d-bar">
                <i style="width: {(Math.min(t.current, allocMax) / allocMax) * 100}%"></i>
                <u style="left: {(Math.min(t.target, allocMax) / allocMax) * 100}%"></u>
              </div>
              <span class="d-alloc-val">
                <b>{formatFloat(t.current, 0)}%</b>
                <span class="d-muted">/ {formatFloat(t.target, 0)}%</span>
              </span>
            </div>
          {/each}
        </div>
        <p class="d-muted d-legend">
          <u></u> marks your target. Amber means 5 or more points away.
        </p>
      {:else}
        <p class="d-muted">
          Set allocation targets in <a href="/more/config">configuration</a> to see how your portfolio
          compares.
        </p>
      {/if}
    </div>
  </div>

  <!-- 2. Does anything need me? -->
  <div class="d-card d-alerts">
    <div class="d-card-head">Needs your attention</div>
    {#if _.isEmpty(alerts)}
      <p class="d-ok">
        <i class="fas fa-circle-check" /> All clear. No overdue bills, allocation drift or data issues.
      </p>
    {:else}
      {#each _.take(alerts, 5) as a}
        <a class="d-alert {a.tone}" href={a.href}>
          <i
            class="fas {a.tone == 'danger'
              ? 'fa-circle-exclamation'
              : a.tone == 'warning'
                ? 'fa-triangle-exclamation'
                : 'fa-circle-info'}"
          />
          <span>{a.text}</span>
          <i class="fas fa-angle-right d-chev" />
        </a>
      {/each}
      {#if alerts.length > 5}<p class="d-muted">and {alerts.length - 5} more</p>{/if}
    {/if}
  </div>

  <!-- 3. Am I on track this month? -->
  <div class="d-grid-2">
    <div class="d-card">
      <div class="d-card-head">
        <a href="/expense/monthly">{now().format("MMMM")} so far</a>
      </div>
      <div class="d-stats">
        <div>
          <span class="stat-label">Income</span><b class="stat-value"
            >{formatCurrencyCrude(income)}</b
          >
        </div>
        <div>
          <span class="stat-label">Spent</span><b class="stat-value">{formatCurrencyCrude(spent)}</b
          >
        </div>
        <div>
          <span class="stat-label">Saved</span>
          <b
            class="stat-value"
            class:is-pos={savingsRate !== null && savingsRate >= 0}
            class:is-neg={savingsRate !== null && savingsRate < 0}
          >
            {savingsRate === null ? "n/a" : formatFloat(savingsRate, 0) + "%"}
          </b>
        </div>
      </div>

      <div class="d-sub">Where it went</div>
      <ZeroState item={spendRows}>No spending this month yet.</ZeroState>
      <div class="d-fill">
        {#each spendRows as r}
          <div class="d-bar-row">
            <div class="d-row-top">
              <span class="truncate">{r.name}</span>
              <span>
                {formatCurrencyCrude(r.actual)}
                <span class="d-muted"> · {formatFloat((r.actual / (spent || 1)) * 100, 0)}%</span>
              </span>
            </div>
            <div class="d-bar"><i style="width: {(r.actual / maxSpend) * 100}%"></i></div>
          </div>
        {/each}
      </div>
    </div>

    <div class="d-stack">
      <div class="d-card">
        <div class="d-card-head"><a href="/cash_flow/monthly">Income vs spending</a></div>
        <ZeroState item={cashFlows}>No transactions in the last 3 months.</ZeroState>
        {#each cashFlows as c}
          <div class="d-flow">
            <span class="d-flow-month">{c.date.format("MMM")}</span>
            <div>
              <div class="d-flow-bar inc" style="width: {(c.income / flowMax) * 100}%"></div>
              <div
                class="d-flow-bar exp"
                style="width: {(Math.abs(c.expenses) / flowMax) * 100}%"
              ></div>
            </div>
            <span class="d-flow-val">
              <span class="inc">{formatCurrencyCrude(c.income)}</span>
              <span class="exp">{formatCurrencyCrude(Math.abs(c.expenses))}</span>
            </span>
          </div>
        {/each}
      </div>

      {#if !_.isEmpty(upcoming)}
        <div class="d-card">
          <div class="d-card-head"><a href="/cash_flow/recurring">Upcoming bills</a></div>
          {#each upcoming as u}
            {@const late = u.next.scheduled.isBefore(now())}
            <div class="d-list-row">
              <span class="truncate">{u.ts.key}</span>
              <span class="d-muted" class:late
                >{late ? "overdue" : u.next.scheduled.format("DD MMM")}</span
              >
              <b>{formatCurrencyCrude(totalRecurring(u.ts))}</b>
            </div>
          {/each}
        </div>
      {/if}
    </div>
  </div>

  <!-- 4. Am I heading where I want to? -->
  {#if !_.isEmpty(goals)}
    <div class="d-card">
      <div class="d-card-head"><a href="/more/goals">Goals</a></div>
      <div class="d-goals">
        {#each goals as g}
          <a class="d-goal" href="/more/goals">
            <div class="d-row-top">
              <span>
                {#if g.icon}<span class="custom-icon">{iconGlyph(g.icon)}</span>{/if}
                {g.name}
              </span>
              <b>{formatPercentage(goalPercent(g) / 100)}</b>
            </div>
            <div class="d-bar good"><i style="width: {goalPercent(g)}%"></i></div>
            <div class="d-row-top d-muted">
              <span>{formatCurrencyCrude(g.current)} of {formatCurrencyCrude(g.target)}</span>
              <span>{goalEta(g)}</span>
            </div>
          </a>
        {/each}
      </div>
    </div>
  {/if}

  <!-- 5. What just happened? -->
  {#if !_.isEmpty(transactions)}
    <div class="d-card">
      <div class="d-card-head">
        <a href="/ledger/transaction">Recent activity</a>
        <a class="d-muted" href="/ledger/transaction">View all</a>
      </div>
      {#each _.take(transactions, 6) as t}
        {@const a = activity(t)}
        <div class="d-list-row">
          <span class="d-date">{t.date.format("DD MMM")}</span>
          <span class="d-grow">
            <span class="truncate d-title">{t.payee}</span>
            <span class="d-muted truncate d-sub-line">{a.accounts}</span>
          </span>
          <b>{formatCurrency(a.amount)}</b>
        </div>
      {/each}
    </div>
  {/if}
</section>
