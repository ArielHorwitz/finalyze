<!--
  The reconciliation bridge of the available balance and the net worth: how
  each went from the period's start to the end of its last month (opening +
  incomes − expenses − set aside + transfers + reconciliations = closing), then
  to the balances' date. Every term shows its rows. `compact` is the available
  balance's only, on one line (without reconciliations when there are none).
-->
<script lang="ts">
  import type { Dashboard, DashboardSelection } from "../../lib/api";
  import { dateLabel, formatWhole, type Drill } from "../../lib/dashboard";
  import { FLOW_KINDS } from "../../lib/kinds";
  import Term from "../Term.svelte";

  let {
    bridge,
    ondrill,
    compact = false,
  }: { bridge: Dashboard["bridge"]; ondrill: (drill: Drill) => void; compact?: boolean } = $props();

  type Line = Dashboard["bridge"][number];
  type BridgeTerm = "incomes" | "expenses" | "saved" | "transfers" | "reconciliations" | "later";
  const NAMES = { available: "Available", net_worth: "Net worth" } as const;
  const TERMS: { key: Exclude<BridgeTerm, "later">; label: string; sign: 1 | -1; title: string }[] = [
    { key: "incomes", label: "+ incomes", sign: 1, title: "Incomes of regular and one-off tags" },
    { key: "expenses", label: "− expenses", sign: -1, title: "Expenses of regular and one-off tags" },
    {
      key: "saved",
      label: "− set aside",
      sign: -1,
      title:
        "Minus the rows of saving tags: from the spending groups for the available balance. For the net worth, both sides of a saving cancel out: what is left is a saving with one side recorded.",
    },
    {
      key: "transfers",
      label: "± transfers",
      sign: 1,
      title:
        "Rows of transfer tags: between the spending groups and others for the available balance. For the net worth, 0 when both sides of every transfer are recorded: what is left is a transfer with one side recorded (e.g. cash taken out with no wallet).",
    },
    {
      key: "reconciliations",
      label: "± reconciliations",
      sign: 1,
      title: "Rows of reconciliation tags: re-anchored balances, with no other side (e.g. opening balances, or a wallet counted short of its rows).",
    },
  ];

  function selection(line: Line, term: BridgeTerm): Partial<DashboardSelection> {
    const base = { line: line.line };
    if (term === "incomes" || term === "expenses") return { ...base, flow: term, kinds: FLOW_KINDS };
    if (term === "saved") return line.line === "available" ? { ...base, flow: "saved" } : { ...base, kinds: ["saving"] };
    if (term === "transfers") return { ...base, kinds: ["transfer"] };
    if (term === "reconciliations") return { ...base, kinds: ["reconciliation"] };
    return { ...base, after_months: true };
  }

  function drill(line: Line, term: BridgeTerm, label: string) {
    const span = term === "later" ? `${dateLabel(addDay(line.end))} – ${dateLabel(line.as_of!)}` : `${dateLabel(line.start)} – ${dateLabel(line.end)}`;
    ondrill({ title: `${NAMES[line.line]} · ${label.replace(/^[+−±] /, "")} · ${span}`, selection: selection(line, term) });
  }

  function addDay(date: string): string {
    const next = new Date(`${date}T00:00:00Z`);
    next.setUTCDate(next.getUTCDate() + 1);
    return next.toISOString().slice(0, 10);
  }

  const lines = $derived(compact ? bridge.filter((line) => line.line === "available") : bridge);
</script>

{#if bridge.length === 0}
  <p class="faint">No months in the period.</p>
{:else if compact}
  {#each lines as line}
    <p class="compact">
      <span class="faint"><Term key="bridge" context="Available: the opening balance, then each term, then the closing balance">Available {dateLabel(line.start)}</Term></span>
      <span class="number">{formatWhole(line.opening)}</span>
      {#each TERMS.filter((term) => term.key !== "reconciliations" || line.reconciliations) as term}
        {@const value = line[term.key] * term.sign}
        <button class="term" onclick={() => drill(line, term.key, term.label)}>
          <span class="number" class:income={value > 0} class:expense={value < 0}>{formatWhole(value, { signed: true })}</span>
          <span class="faint"><Term key="bridge" context={term.title}>{term.label.slice(2)}</Term></span>
        </button>
      {/each}
      <span class="faint">= {dateLabel(line.end)}</span>
      <span class="number">{formatWhole(line.closing)}</span>
      {#if line.later !== null && line.as_of}
        <button class="term" onclick={() => drill(line, "later", "since")} title="The rows after the period's months, up to the balances' date">
          <span class="number">{formatWhole(line.later, { signed: true })}</span>
          <span class="faint"><Term key="bridge" context="The rows after the period's months, up to the balances' date">to {dateLabel(line.as_of)}</Term> =</span>
          <span class="number">{formatWhole(line.closing + line.later)}</span>
        </button>
      {/if}
    </p>
  {/each}
{:else}
  <table class="grid bridge">
    <thead>
      <tr>
        <th></th>
        {#each lines as line}<th class="amount">{#if line.line === "available"}<Term key="available">Available</Term>{:else}<Term key="net_worth">Net worth</Term>{/if}</th>{/each}
      </tr>
    </thead>
    <tbody>
      <tr class="edge">
        <td><Term key="bridge">Opening</Term> <span class="faint">· before {dateLabel(lines[0].start)}</span></td>
        {#each lines as line}<td class="amount">{formatWhole(line.opening)}</td>{/each}
      </tr>
      {#each TERMS as term}
        <tr>
          <td><Term key="bridge" context={term.title}>{term.label}</Term></td>
          {#each lines as line}
            {@const value = line[term.key] * term.sign}
            <td class="amount">
              <button class="cell" class:income={value > 0} class:expense={value < 0} onclick={() => drill(line, term.key, term.label)} title="Show the rows">
                {formatWhole(value, { signed: true })}
              </button>
            </td>
          {/each}
        </tr>
      {/each}
      <tr class="edge">
        <td><Term key="bridge">Closing</Term> <span class="faint">· end of {dateLabel(lines[0].end)}</span></td>
        {#each lines as line}
          <td class="amount" title={line.rounding ? `${formatWhole(line.rounding, { signed: true })} of rounding` : "The terms add up to the cent"}>{formatWhole(line.closing)}</td>
        {/each}
      </tr>
      <tr class="note">
        <td class="faint"><Term key="bridge_one_offs">of which one-offs</Term> <span class="faint">(in incomes and expenses)</span></td>
        {#each lines as line}<td class="amount one-off">{line.one_offs ? formatWhole(line.one_offs, { signed: true }) : ""}</td>{/each}
      </tr>
      {#if lines.some((line) => line.later !== null)}
        <tr>
          <td><Term key="bridge" context="The rows after the period's months, up to the balances' date">Then, to {dateLabel(lines.find((line) => line.as_of)?.as_of ?? lines[0].end)}</Term></td>
          {#each lines as line}
            <td class="amount">
              {#if line.later !== null}
                <button class="cell" class:income={line.later > 0} class:expense={line.later < 0} onclick={() => drill(line, "later", "after the months")} title="Show the rows">
                  {formatWhole(line.later, { signed: true })}
                </button>
              {/if}
            </td>
          {/each}
        </tr>
        <tr class="edge">
          <td><Term key="as_of">Balance</Term> <span class="faint">· as of</span></td>
          {#each lines as line}<td class="amount">{formatWhole(line.closing + (line.later ?? 0))}</td>{/each}
        </tr>
      {/if}
    </tbody>
  </table>
{/if}

<style>
  .bridge td {
    padding: 4px 8px;
  }
  .bridge tr.edge td {
    font-weight: 600;
    background: var(--surface-2);
  }
  .bridge tr.note td {
    font-size: 12px;
    padding-top: 1px;
  }
  .cell,
  .term {
    background: none;
    border: none;
    padding: 0 2px;
    font: inherit;
    color: inherit;
    font-variant-numeric: tabular-nums;
  }
  .cell:hover,
  .cell:focus-visible,
  .term:hover,
  .term:focus-visible {
    text-decoration: underline;
  }
  .one-off {
    color: var(--warning);
  }
  .compact {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 4px 8px;
    margin: 0;
    font-size: 13px;
  }
  .number {
    font-variant-numeric: tabular-nums;
  }
</style>
