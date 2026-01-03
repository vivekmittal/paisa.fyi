<script lang="ts">
  import {
    ajax,
    formatCurrency,
    formatFloat,
    isMobile,
    type Legend,
    type Networth,
    type AssetDistribution
  } from "$lib/utils";
  import COLORS from "$lib/colors";
  import { renderNetworth } from "$lib/networth";
  import * as assetDistribution from "$lib/asset_distribution";
  import _ from "lodash";
  import { onDestroy, onMount, tick } from "svelte";
  import { dateRange, setAllowedDateRange } from "../../../../store";
  import LevelItem from "$lib/components/LevelItem.svelte";
  import ZeroState from "$lib/components/ZeroState.svelte";
  import BoxLabel from "$lib/components/BoxLabel.svelte";
  import LegendCard from "$lib/components/LegendCard.svelte";

  let networth = 0;
  let investment = 0;
  let gain = 0;
  let xirr = 0;
  let svg: Element;
  let destroy: () => void;
  let points: Networth[] = [];
  let legends: Legend[] = [];
  let assetDistributions: AssetDistribution[] = [];
  let assetDistributionLegends: Legend[] = [];
  let assetDistributionRenderer: (data: AssetDistribution[]) => void;

  $: if (!_.isEmpty(points)) {
    if (destroy) {
      destroy();
    }

    ({ destroy, legends } = renderNetworth(
      _.filter(
        points,
        (p) => p.date.isSameOrBefore($dateRange.to) && p.date.isSameOrAfter($dateRange.from)
      ),
      svg
    ));
  }

  onDestroy(async () => {
    if (destroy) {
      destroy();
    }
  });

  onMount(async () => {
    const result = await ajax("/api/networth");
    points = result.networthTimeline;
    setAllowedDateRange(_.map(points, (p) => p.date));

    const current = _.last(points);
    if (current) {
      networth = current.investmentAmount + current.gainAmount - current.withdrawalAmount;
      investment = current.investmentAmount - current.withdrawalAmount;
      gain = current.gainAmount;
    }
    xirr = result.xirr;
    assetDistributions = result.assetDistribution || [];

    if (!_.isEmpty(assetDistributions)) {
      await tick(); // Wait for DOM to update with the SVG element
      const { renderer, legends } = assetDistribution.renderAssetDistribution(
        "#d3-asset-distribution",
        assetDistributions
      );
      assetDistributionRenderer = renderer;
      assetDistributionLegends = legends;
    }
  });
</script>

<section class="section tab-networth">
  <div class="container is-fluid">
    <nav class="level {isMobile() && 'grid-2'}">
      <LevelItem title="Net worth" color={COLORS.primary} value={formatCurrency(networth)} />
      <LevelItem
        title="Net Investment"
        color={COLORS.secondary}
        value={formatCurrency(investment)}
      />
      <LevelItem
        title="Gain / Loss"
        color={gain >= 0 ? COLORS.gainText : COLORS.lossText}
        value={formatCurrency(gain)}
      />
      <LevelItem title="XIRR" value={formatFloat(xirr)} />
    </nav>
  </div>
</section>

<section class="section tab-networth">
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12">
        <div class="box overflow-x-auto">
          <ZeroState item={points}>
            <strong>Oops!</strong> You have no transactions.
          </ZeroState>

          <LegendCard {legends} clazz="ml-4" />
          <svg id="d3-networth-timeline" height="500" bind:this={svg} />
        </div>
      </div>
    </div>
    <BoxLabel text="Networth Timeline" />
  </div>
</section>

{#if !_.isEmpty(assetDistributions)}
  <section class="section tab-networth">
    <div class="container is-fluid">
      <div class="columns">
        <div class="column is-12">
          <div class="box px-2 pb-0">
            <LegendCard legends={assetDistributionLegends} clazz="mb-2 mt-2 overflow-x-auto" />
            <svg
              id="d3-asset-distribution"
              height="250"
              width="100%"
            />
          </div>
        </div>
      </div>
      <BoxLabel text="Asset Distribution" />
    </div>
  </section>
{/if}
