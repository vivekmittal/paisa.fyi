<script lang="ts">
  import { ajax, type Networth, type AssetDistribution, type Legend } from "$lib/utils";
  import * as assetDistribution from "$lib/asset_distribution";
  import _ from "lodash";
  import { onMount, tick } from "svelte";
  import { setAllowedDateRange } from "../../../../store";
  import BoxLabel from "$lib/components/BoxLabel.svelte";
  import LegendCard from "$lib/components/LegendCard.svelte";
  import NetworthCurrency from "$lib/components/NetworthCurrency.svelte";
  import { selectedCurrency } from "../../../../persisted_store";

  let points: Networth[] = [];
  let currencyNetworths: {
    name: string;
    symbol: string;
    commodity: string;
    networthTimeline: Networth[];
  }[] = [];
  $: selected = _.find(currencyNetworths, (c) => c.name === $selectedCurrency);
  let assetDistributions: AssetDistribution[] = [];
  let assetDistributionLegends: Legend[] = [];
  let assetDistributionRenderer: (data: AssetDistribution[]) => void;

  onMount(async () => {
    const result = await ajax("/api/networth");
    points = result.networthTimeline;
    currencyNetworths = result.currencyNetworths || [];
    setAllowedDateRange(_.map(points, (p) => p.date));

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

{#key $selectedCurrency}
  {#if selected}
    <NetworthCurrency
      name={selected.name}
      symbol={selected.symbol}
      points={selected.networthTimeline}
      emptyMessage="No prices found for commodity {selected.commodity}."
    />
  {:else}
    <NetworthCurrency name={USER_CONFIG.default_currency} {points} />
  {/if}
{/key}

{#if !_.isEmpty(assetDistributions)}
  <section class="section tab-networth">
    <div class="container is-fluid">
      <div class="columns">
        <div class="column is-12">
          <div class="box px-2 pb-0">
            <LegendCard legends={assetDistributionLegends} clazz="mb-2 mt-2 overflow-x-auto" />
            <svg id="d3-asset-distribution" height="250" width="100%" />
          </div>
        </div>
      </div>
      <BoxLabel text="Asset Distribution" />
    </div>
  </section>
{/if}
