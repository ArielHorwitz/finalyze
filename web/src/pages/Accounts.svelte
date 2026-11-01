<!--
  Accounts in their groups. A group's role says what its accounts hold:
  day-to-day money (spending) or savings, investments and debt (holding, only
  in the net worth). Groups are created, renamed, reordered, colored and given
  a role here, and accounts are moved between them. "Balance on" shows the
  balance of an account, a source, a group or the net worth at the end of a
  date (e.g. to work out a reconciliation).
-->
<script lang="ts">
  import { onMount } from "svelte";
  import Amount from "../components/Amount.svelte";
  import DateInput from "../components/DateInput.svelte";
  import Modal from "../components/Modal.svelte";
  import { api, type Account, type BalanceQuery, type Group, type Schemas } from "../lib/api";
  import { Cursor, scrollToIndex } from "../lib/cursor.svelte";
  import { showDate } from "../lib/display.svelte";
  import { GROUP_ROLES, nextOf, type GroupRole } from "../lib/kinds";
  import { app, confirm, keyName, refreshAccounts, toast, toastError } from "../lib/state.svelte";
  import { bind, useKeys } from "../lib/use-keys.svelte";
  import Term from "../components/Term.svelte";

  interface Item {
    group: Group;
    account: Account | null;
  }
  type Dialog = "account" | "group" | "create";

  const items: Item[] = $derived(
    app.groups.flatMap((group) => [
      { group, account: null },
      ...app.accounts
        .filter((account) => account.group === group.name)
        .map((account) => ({ group, account })),
    ]),
  );
  const cursor = new Cursor(() => items.length);
  const current = $derived(items[cursor.index] as Item | undefined);
  let container: HTMLDivElement | undefined = $state();
  let dialog = $state<Dialog | null>(null);
  $effect(() => scrollToIndex(container, cursor.index));

  // Dialog fields
  let name = $state("");
  let role = $state<GroupRole>("spending");
  let color = $state("");
  let groupName = $state("");

  const keyOf = (item: Item) => (item.account ? `account:${item.account.name}` : `group:${item.group.name}`);
  const groupNames = $derived(app.groups.map((group) => group.name));

  async function run(action: () => Promise<unknown>, message: string, select?: string) {
    try {
      await action();
      await refreshAccounts();
      toast(message, "success");
      dialog = null;
      if (select) {
        const index = items.findIndex((item) => keyOf(item) === select);
        if (index >= 0) cursor.moveTo(index);
      }
      return true;
    } catch (error) {
      toastError(error);
      return false;
    }
  }

  function moveAccount(account: Account, group: string) {
    if (group === account.group) return;
    run(() => api.updateAccount(account.name, { group }), `${account.name} is in ${group}`, `account:${account.name}`);
  }

  function toggleRole(group: Group) {
    const next = nextOf(
      GROUP_ROLES.map((item) => item.id),
      group.role,
    );
    run(() => api.updateGroup(group.name, { role: next }), `${group.name} is for ${next}`, `group:${group.name}`);
  }

  /** x: the next group of an account, or the other role of a group. */
  function cycle() {
    const item = current;
    if (!item) return;
    if (item.account) moveAccount(item.account, nextOf(groupNames, item.account.group));
    else toggleRole(item.group);
  }

  function moveGroup(step: number) {
    const group = current?.group;
    if (!group || current?.account) return;
    const position = group.position + step;
    if (position < 0 || position >= app.groups.length) return;
    run(() => api.updateGroup(group.name, { position }), `Moved ${group.name}`, `group:${group.name}`);
  }

  function open(kind: Dialog) {
    const item = current;
    if (kind === "create") {
      name = "";
      role = "holding";
      color = "";
    } else if (!item) {
      return;
    } else if (item.account) {
      kind = "account";
      color = item.account.color ?? "";
      groupName = item.account.group;
    } else {
      kind = "group";
      name = item.group.name;
      role = item.group.role;
      color = item.group.color ?? "";
    }
    dialog = kind;
  }

  function validColor(value: string): boolean {
    if (value && !/^#?[0-9a-f]{3}([0-9a-f]{3})?$/i.test(value) && !/^[a-z]+$/i.test(value)) {
      toast("Colors look like #3987e5", "error");
      return false;
    }
    return true;
  }

  function save() {
    const item = current;
    const value = color.trim() || null;
    if (!validColor(value ?? "")) return;
    if (dialog === "create") {
      if (!name.trim()) return;
      run(() => api.createGroup({ name: name.trim(), role, color: value }), `Created ${name.trim()}`, `group:${name.trim()}`);
    } else if (dialog === "account" && item?.account) {
      const account = item.account;
      run(
        () => api.updateAccount(account.name, { group: groupName, color: value }),
        `Updated ${account.name}`,
        `account:${account.name}`,
      );
    } else if (dialog === "group" && item) {
      const group = item.group;
      if (!name.trim()) return;
      run(
        () => api.updateGroup(group.name, { new_name: name.trim(), role, color: value }),
        `Updated ${name.trim()}`,
        `group:${name.trim()}`,
      );
    }
  }

  async function remove() {
    const item = current;
    if (!item || item.account) return;
    const group = item.group;
    if (group.accounts.length) {
      toast(`${group.name} has accounts: move them to another group first`, "error");
      return;
    }
    if (!(await confirm(`Delete the group ${group.name}?`, { title: "Delete", confirmLabel: "Delete", danger: true }))) return;
    run(() => api.deleteGroup(group.name), `Deleted ${group.name}`);
  }

  useKeys("Accounts", () => [
    ...cursor.bindings(),
    bind("accounts.cycle", cycle),
    bind("accounts.edit", () => open("group")),
    bind("reorder.up", () => moveGroup(-1), { description: "Move the group up" }),
    bind("reorder.down", () => moveGroup(1), { description: "Move the group down" }),
    bind("accounts.create", () => open("create")),
    bind("accounts.delete", remove),
  ]);

  onMount(() => {
    refreshAccounts().catch(toastError);
  });

  // Balance on a date: of the net worth (no query), a group, an account or a
  // source. The date is the latest data's until one is typed.
  let balanceDate = $state("");
  let balanceDateInvalid = $state(false);
  let balanceOf = $state("{}");
  let balance = $state<Schemas["BalanceOnDate"] | null>(null);
  let balanceRequest = 0;
  const ofValue = (query: BalanceQuery) => JSON.stringify(query);

  $effect(() => {
    const query = { ...JSON.parse(balanceOf), date: balanceDate || null };
    if (balanceDateInvalid) return;
    const request = ++balanceRequest;
    api
      .balance(query)
      .then((result) => {
        if (request === balanceRequest) balance = result;
      })
      .catch((error) => {
        if (request === balanceRequest) {
          balance = null;
          toastError(error);
        }
      });
  });

  const roleDescription = (value: GroupRole) => GROUP_ROLES.find((item) => item.id === value)?.description ?? "";
</script>

<div class="page">
  <div class="page-header">
    <h1>Accounts</h1>
    <span class="muted summary">
      Spending groups are day-to-day money (cash flows, breakdowns, the available balance). Holding groups (savings,
      investments, debt) only count in the net worth.
    </span>
    <div class="spacer"></div>
    <button onclick={() => open("create")} title={keyName("accounts.create")}>New group</button>
  </div>
  <div class="balance-on">
    <Term key="balance_on_date">Balance on</Term>
    <DateInput bind:value={balanceDate} bind:invalid={balanceDateInvalid} placeholder="latest" aria-label="Balance date" />
    <span class="muted">of</span>
    <select bind:value={balanceOf} aria-label="Balance of">
      <option value={ofValue({})}>net worth</option>
      <optgroup label="Groups">
        {#each app.groups as group (group.name)}<option value={ofValue({ group: group.name })}>{group.name}</option>{/each}
      </optgroup>
      <optgroup label="Accounts and sources">
        {#each app.accounts as account (account.name)}
          <option value={ofValue({ account: account.name })}>{account.name}</option>
          {#each account.sources as source (source)}
            <option value={ofValue({ account: account.name, source })}>{account.name}/{source}</option>
          {/each}
        {/each}
      </optgroup>
    </select>
    {#if balance}
      <span class="balance-result" data-testid="balance-on-date">
        <Amount value={balance.balance} />
        <span class="muted">
          at the end of {showDate(balance.date)} · {balance.rows} row{balance.rows === 1 ? "" : "s"}{balance.last_date ? `, the last on ${showDate(balance.last_date)}` : ""}
        </span>
      </span>
    {/if}
  </div>
  <div class="scroll" bind:this={container}>
    <table class="grid">
      <thead>
        <tr><th><Term key="account">Name</Term></th><th><Term key="account_groups">Group</Term></th><th>Color</th><th class="num">Transactions</th><th></th></tr>
      </thead>
      <tbody>
        {#each items as item, index (keyOf(item))}
          <tr
            data-index={index}
            class:active={index === cursor.index}
            class:group-row={!item.account}
            onmousedown={() => cursor.moveTo(index)}
            ondblclick={() => open("group")}
          >
            {#if item.account}
              {@const account = item.account}
              <td class="name"><span class="indent">{account.name}</span></td>
              <td>
                <select
                  tabindex="-1"
                  value={account.group}
                  aria-label="Group of {account.name}"
                  onchange={(event) => moveAccount(account, event.currentTarget.value)}
                >
                  {#each app.groups as group (group.name)}<option value={group.name}>{group.name}</option>{/each}
                </select>
              </td>
              <td>
                {#if account.color}<span class="swatch" style:background={account.color}></span>{/if}
                <span class:faint={!account.color}>{account.color ?? "none"}</span>
              </td>
              <td class="num">{account.transactions}</td>
              <td class="row-actions"><button class="link" onclick={() => { cursor.moveTo(index); open("account"); }}>edit</button></td>
            {:else}
              {@const group = item.group}
              <td class="name">{group.name}</td>
              <td>
                <span class="role" class:holding={group.role === "holding"}><Term key="role_{group.role}" quiet context={roleDescription(group.role)}>{group.role}</Term></span>
                <span class="faint">{group.accounts.length} account{group.accounts.length === 1 ? "" : "s"}</span>
              </td>
              <td>
                {#if group.color}<span class="swatch" style:background={group.color}></span>{/if}
                <span class:faint={!group.color}>{group.color ?? "none"}</span>
              </td>
              <td class="num"></td>
              <td class="row-actions">
                <button class="link" onclick={() => { cursor.moveTo(index); open("group"); }}>edit</button>
                {#if !group.accounts.length}
                  <button class="link danger-link" onclick={() => { cursor.moveTo(index); remove(); }}>delete</button>
                {/if}
              </td>
            {/if}
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>

{#snippet colorField(placeholder: string)}
  <span class="field-label">Color</span>
  <div class="color-row">
    <input bind:value={color} {placeholder} />
    <input type="color" tabindex="-1" value={/^#[0-9a-f]{6}$/i.test(color) ? color : "#888888"} oninput={(event) => (color = event.currentTarget.value)} />
    {#if color}<button class="link" onclick={() => (color = "")}>clear</button>{/if}
  </div>
{/snippet}

{#snippet roleField()}
  <span class="field-label">Role</span>
  <select bind:value={role}>
    {#each GROUP_ROLES as item (item.id)}<option value={item.id}>{item.label}: {item.description}</option>{/each}
  </select>
{/snippet}

{#if dialog}
  {@const title =
    dialog === "create" ? "New group" : dialog === "account" ? `Account ${current?.account?.name}` : `Group ${current?.group.name}`}
  <Modal {title} onclose={() => (dialog = null)} width="520px" bindings={() => [bind("dialog.confirm", save, { description: "Save", inInputs: true })]}>
    <div class="form">
      {#if dialog === "account"}
        <span class="field-label">Group</span>
        <select bind:value={groupName}>
          {#each app.groups as group (group.name)}<option value={group.name}>{group.name} ({group.role})</option>{/each}
        </select>
      {:else}
        <span class="field-label">Name</span>
        <input bind:value={name} placeholder="e.g. savings" />
        {@render roleField()}
      {/if}
      {@render colorField("none, e.g. #3987e5")}
    </div>
    {#snippet footer()}
      <button onclick={() => (dialog = null)}>Cancel</button>
      <button class="primary" onclick={save}>{dialog === "create" ? "Create" : "Save"} <kbd>Enter</kbd></button>
    {/snippet}
  </Modal>
{/if}

<style>
  .balance-on {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    padding: 6px 0 10px;
  }
  .balance-on :global(input) {
    width: 110px;
  }
  .balance-result {
    font-variant-numeric: tabular-nums;
  }
  .summary {
    font-size: 12px;
  }
  table.grid td {
    padding: 5px 8px;
  }
  .group-row td {
    border-top: 1px solid var(--border-strong);
  }
  .group-row .name {
    font-weight: 600;
  }
  .indent {
    padding-left: 18px;
  }
  .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .role {
    display: inline-block;
    min-width: 64px;
    color: var(--income);
  }
  .role.holding {
    color: var(--accent);
  }
  select {
    min-width: 140px;
  }
  .row-actions {
    text-align: right;
    white-space: nowrap;
  }
  .row-actions button {
    opacity: 0;
    margin-left: 10px;
  }
  tr:hover .row-actions button,
  tr.active .row-actions button,
  .row-actions button:focus-visible {
    opacity: 1;
  }
  .danger-link {
    color: var(--danger);
  }
  .form {
    display: grid;
    grid-template-columns: 70px 1fr;
    gap: 10px;
    align-items: center;
  }
  .field-label {
    color: var(--muted);
  }
  .color-row {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  input[type="color"] {
    width: 34px;
    height: 26px;
    padding: 1px;
  }
</style>
