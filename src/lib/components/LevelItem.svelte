<script lang="ts">
  import _ from "lodash";

  // The one stat component. Styling lives in app.scss (.stat-label, .stat-value, .stat-sub).
  export let title: string;
  export let value: string;
  export let small: boolean = false;
  export let narrow: boolean = false;
  export let color: string = null;
  export let subtitle: string = null;
</script>

<div class="level-item {narrow ? 'is-narrow' : ''} has-text-left">
  <div>
    <p class="stat-label">{title}</p>
    <p class="stat-value" class:is-sm={small} style={color ? `color: ${color}` : ""}>{value}</p>
    {#if !_.isEmpty(subtitle)}
      <div class="stat-sub">{@html subtitle}</div>
    {/if}
  </div>
</div>
