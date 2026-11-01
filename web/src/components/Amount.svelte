<!-- A signed amount, colored as income (positive) or expense (negative). -->
<script lang="ts">
  import { formatAmount } from "../lib/money";

  let { value }: { value: number } = $props();
</script>

<span class="amount" class:income={value > 0} class:expense={value < 0}>
  {formatAmount(value, { signed: true })}
</span>
