<script lang="ts">
  import { formatCurrency, formatFloat, isMobile, type Legend, type Networth } from "$lib/utils";
  import COLORS from "$lib/colors";
  import { renderNetworth, computePeriodStats } from "$lib/networth";
  import _ from "lodash";
  import { onDestroy } from "svelte";
  import { dateRange } from "../../store";
  import LevelItem from "$lib/components/LevelItem.svelte";
  import ZeroState from "$lib/components/ZeroState.svelte";
  import BoxLabel from "$lib/components/BoxLabel.svelte";
  import LegendCard from "$lib/components/LegendCard.svelte";

  export let name: string;
  export let symbol = "";
  export let points: Networth[] = [];
  export let emptyMessage = "You have no transactions.";

  let svg: Element;
  let destroy: () => void;
  let legends: Legend[] = [];
  let stats = { networth: 0, investment: 0, gain: 0, xirr: 0 };

  const fmt = (v: number) =>
    symbol + formatCurrency(v, symbol ? 2 : null, symbol ? "en-US" : undefined);

  $: if (!_.isEmpty(points) && svg) {
    if (destroy) {
      destroy();
    }

    const inRange = (p: Networth) =>
      p.date.isSameOrBefore($dateRange.to) && p.date.isSameOrAfter($dateRange.from);
    ({ destroy, legends } = renderNetworth(_.filter(points, inRange), svg, {
      currencySymbol: symbol
    }));
    stats = computePeriodStats(points, $dateRange.from, $dateRange.to);
  }

  onDestroy(() => {
    if (destroy) {
      destroy();
    }
  });
</script>

<section class="section tab-networth">
  <div class="container is-fluid">
    <nav class="level {isMobile() && 'grid-2'}">
      <LevelItem title="Net worth" color={COLORS.primary} value={fmt(stats.networth)} />
      <LevelItem title="Net Investment" color={COLORS.secondary} value={fmt(stats.investment)} />
      <LevelItem
        title="Gain / Loss"
        color={stats.gain >= 0 ? COLORS.gainText : COLORS.lossText}
        value={fmt(stats.gain)}
      />
      <LevelItem title="XIRR" value={formatFloat(stats.xirr)} />
    </nav>
  </div>
</section>

<section class="section tab-networth">
  <div class="container is-fluid">
    <div class="columns">
      <div class="column is-12">
        <div class="box overflow-x-auto">
          <ZeroState item={points}>
            <strong>Oops!</strong>
            {emptyMessage}
          </ZeroState>

          <LegendCard {legends} clazz="ml-4" />
          <svg height="500" bind:this={svg} />
        </div>
      </div>
    </div>
    <BoxLabel text="Networth Timeline ({name})" />
  </div>
</section>
