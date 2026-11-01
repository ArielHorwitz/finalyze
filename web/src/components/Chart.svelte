<!--
  An ECharts chart that fills its box, follows its size, and updates (with
  animation) when `option` changes. Series toggled in the legend stay toggled.
-->
<script lang="ts">
  import { onMount } from "svelte";
  import { echarts, type ChartClick, type EChartsOption } from "../lib/charts";

  let {
    option,
    onclick,
    height = "320px",
    label = "Chart",
  }: {
    option: EChartsOption;
    onclick?: (event: ChartClick) => void;
    height?: string;
    label?: string;
  } = $props();

  let element: HTMLDivElement;
  let chart: echarts.ECharts | null = $state(null);

  onMount(() => {
    const instance = echarts.init(element, "finalyze");
    instance.on("click", (event) => onclick?.(event as unknown as ChartClick));
    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(element);
    chart = instance;
    return () => {
      observer.disconnect();
      instance.dispose();
    };
  });

  $effect(() => {
    // Replace the series (their number changes), merge the rest (e.g. the legend's selection)
    chart?.setOption(option, { replaceMerge: ["series", "xAxis", "yAxis"] });
  });
</script>

<div class="chart" class:clickable={!!onclick} style:height bind:this={element} role="img" aria-label={label}></div>

<style>
  .chart {
    width: 100%;
    min-width: 0;
  }
</style>
