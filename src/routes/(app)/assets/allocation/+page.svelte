<script lang="ts">
  import {
    renderAllocation,
    renderAllocationTarget,
    renderAllocationTimeline
  } from "$lib/allocation";
  import COLORS, { generateColorScheme } from "$lib/colors";
  import BoxLabel from "$lib/components/BoxLabel.svelte";
  import LegendCard from "$lib/components/LegendCard.svelte";
  import Table from "$lib/components/Table.svelte";
  import { accountName, nonZeroCurrency } from "$lib/table_formatters";
  import {
    ajax,
    formatPercentage,
    rem,
    type Aggregate,
    type AllocationTarget,
    type Legend
  } from "$lib/utils";
  import _ from "lodash";
  import { onMount, tick } from "svelte";
  import type { ColumnDefinition, ProgressBarParams } from "tabulator-tables";
  import type * as d3 from "d3";

  let showAllocation = false;
  let depth = 2;
  let allocationTimelineLegends: Legend[] = [];
  let aggregateLeafNodes: Aggregate[] = [];
  let total = 0;

  let allocationTargets: AllocationTarget[] = [];
  let colorScale: d3.ScaleOrdinal<string, string>;
  let disabledTargetNames = new Set<string>();

  function toggleTarget(name: string) {
    const next = new Set(disabledTargetNames);
    if (next.has(name)) {
      next.delete(name);
    } else {
      next.add(name);
    }
    disabledTargetNames = next;
    rerenderTargets();
  }

  function rerenderTargets() {
    renderAllocationTarget(allocationTargets, colorScale, {
      disabledNames: disabledTargetNames
    });
  }

  const columns: ColumnDefinition[] = [
    { title: "Account", field: "account", formatter: accountName },
    {
      title: "Market Value",
      field: "market_amount",
      hozAlign: "right",
      formatter: nonZeroCurrency
    },
    {
      title: "Percent",
      field: "percent",
      hozAlign: "right",
      formatter: (cell) => formatPercentage(cell.getValue() / 100, 2)
    },
    {
      title: "%",
      field: "percent",
      hozAlign: "right",
      formatter: "progress",
      cssClass: "has-text-left",
      minWidth: rem(250),
      formatterParams: {
        color: COLORS.assets,
        min: 0
      }
    }
  ];

  onMount(async () => {
    const {
      aggregates: aggregates,
      aggregates_timeline: aggregatesTimeline,
      allocation_targets: targets
    } = await ajax("/api/allocation");
    allocationTargets = targets;
    const accounts = _.keys(aggregates);
    aggregateLeafNodes = _.filter(_.values(aggregates), (a) => a.market_amount > 0);
    total = _.sumBy(aggregateLeafNodes, (a) => a.market_amount);
    aggregateLeafNodes = _.map(aggregateLeafNodes, (a) => {
      a.percent = (a.market_amount / total) * 100;
      return a;
    });
    const max = _.max(_.map(aggregateLeafNodes, (a) => a.percent)) || 100;
    (_.last(columns).formatterParams as ProgressBarParams).max = max;
    const color = generateColorScheme(accounts);
    colorScale = color;
    depth = _.max(_.map(accounts, (account) => account.split(":").length));

    if (!_.isEmpty(allocationTargets)) {
      showAllocation = true;
    }
    await tick();

    rerenderTargets();
    renderAllocation(aggregates, color);
    allocationTimelineLegends = renderAllocationTimeline(aggregatesTimeline);
  });
</script>

<section class="section tab-allocation" style={showAllocation ? "" : "display: none"}>
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12 has-text-centered">
        <div class="box overflow-x-auto">
          {#if allocationTargets.length > 0}
            <div class="target-toggles">
              <span class="target-toggles-label">Include:</span>
              {#each _.sortBy(allocationTargets, (t) => t.name) as t (t.name)}
                <label class="target-toggle" class:is-disabled={disabledTargetNames.has(t.name)}>
                  <input
                    type="checkbox"
                    checked={!disabledTargetNames.has(t.name)}
                    on:change={() => toggleTarget(t.name)}
                  />
                  <span>{t.name}</span>
                </label>
              {/each}
            </div>
          {/if}
          <div id="d3-allocation-target-treemap" style="width: 100%; position: relative" />
          <svg id="d3-allocation-target" />
        </div>
      </div>
    </div>
    <BoxLabel text="Allocation Targets" />
  </div>
</section>
<section class="section tab-allocation">
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12 has-text-centered">
        <div id="d3-allocation-category" style="width: 100%; height: {depth * 100}px" />
      </div>
    </div>
    <BoxLabel text="Allocation by category" />
  </div>
</section>
<section class="section tab-allocation">
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12 has-text-centered">
        <div id="d3-allocation-value" style="width: 100%; height: 300px" />
      </div>
    </div>
    <BoxLabel text="Allocation by value" />
  </div>
</section>
<section class="section tab-allocation">
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12">
        <div class="box">
          <LegendCard legends={allocationTimelineLegends} clazz="ml-4" />
          <svg id="d3-allocation-timeline" width="100%" height="300" />
        </div>
      </div>
    </div>
    <BoxLabel text="Allocation Timeline" />
  </div>
</section>
<section class="section tab-allocation">
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12">
        <Table data={aggregateLeafNodes} tree {columns} />
      </div>
    </div>
    <BoxLabel text="Allocation Table" />
  </div>
</section>

<style>
  .target-toggles {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem 1rem;
    padding: 0.5rem 0.75rem 1rem;
    font-size: 0.875rem;
  }
  .target-toggles-label {
    font-weight: 600;
    color: #666;
    margin-right: 0.25rem;
  }
  .target-toggle {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    cursor: pointer;
    user-select: none;
  }
  .target-toggle input {
    cursor: pointer;
    margin: 0;
  }
  .target-toggle.is-disabled span {
    text-decoration: line-through;
    opacity: 0.5;
  }
</style>
