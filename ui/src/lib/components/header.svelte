<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { KavePeer, KaveStats } from '$lib/kv';
	import { onDestroy } from 'svelte';

	let { client }: { client: KaveClient } = $props();

	let stats = $state<KaveStats | null>(null);
	let error = $state(false);
	// epoch ms
	let lastPoll = $state<number | null>(null);

	async function poll() {
		try {
			stats = await client.stats();
			error = false;
			lastPoll = Date.now();
		} catch {
			error = true;
		}
	}

	poll();
	const interval = setInterval(poll, 2000);
	onDestroy(() => clearInterval(interval));

	const stateLabel: string = $derived(stats?.state ?? '—');
	const isLeader: boolean = $derived(stateLabel === 'Leader');
	const term: number = $derived(stats?.term ?? -1);
	const commitIdx: number = $derived(stats?.commit_index ?? -1);
	const appliedIdx: number = $derived(stats?.applied_index ?? -1);
	const leaderId: string = $derived(stats?.leader_id ?? 'UNKNOWN');
	const fsmPending: number = $derived(stats?.fsm_pending ?? -1);
	const peers: KavePeer[] = $derived(stats?.readable_configuration ?? []);
	const latestConfigIndex: number = $derived(stats?.latest_configuration_index ?? -1);

	// staleness indicator: age of last successful poll in seconds
	const staleMs = $derived(lastPoll ? Date.now() - lastPoll : null);
</script>

<header class="cluster-bar">
	<div class="bar-left">
		<span class="brand">kave</span>
		<span class="sep">/</span>
		<span class="subtitle">raft-backed kv demo</span>
	</div>

	<div class="bar-center">
		{#if error}
			<div class="pill error">
				<span class="dot"></span>
				<span>cluster unreachable</span>
			</div>
		{:else if !stats}
			<div class="pill loading">
				<span class="spinner"></span>
				<span>connecting…</span>
			</div>
		{:else}
			<!-- index of last configuration (peer list) -->
			<div class="pill neutral">
				<span class="pill-label">latest configuration index</span>
				<span class="pill-val">{latestConfigIndex}</span>
			</div>
			
			<!-- cluster state -->
			{#each peers as p (p.id)}
				<div class="pill neutral">
					<span class="pill-label">ID</span>
					<span class="pill-val">{p.id}</span>
				</div>
				<div class="pill neutral">
					<span class="pill-label">addr</span>
					<span class="pill-val">{p.address}</span>
				</div>
				<div class="pill neutral">
					<span class="pill-label">Suffrage</span>
					<span class="pill-val">{p.suffrage}</span>
				</div>
			{/each}

			<!-- leader id — most useful for followers -->
			{#if !isLeader && leaderId !== '—'}
				<div class="pill neutral">
					<span class="pill-label">leader</span>
					<span class="pill-val dim">{leaderId}</span>
				</div>
			{/if}


			<!-- raft term -->
			<div class="pill neutral">
				<span class="pill-label">term</span>
				<span class="pill-val">{term}</span>
			</div>

			<!-- commit / applied -->
			<div class="pill neutral">
				<span class="pill-label">commit</span>
				<span class="pill-val">{commitIdx}</span>
			</div>

			<div class="pill neutral">
				<span class="pill-label">applied</span>
				<span class="pill-val">{appliedIdx}</span>
			</div>

			<!-- fsm_pending: if set, nonzero means fsm is lagging behind -->
			{#if fsmPending <= 0}
				<div class="pill warn">
					<span class="pill-label">fsm lag</span>
					<span class="pill-val">{fsmPending}</span>
				</div>
			{/if}
		{/if}
	</div>

	<div class="bar-right">
		<span
			class="pulse"
			class:stale={error}
			title={lastPoll ? `last poll ${Math.round((staleMs ?? 0) / 1000)}s ago` : 'never polled'}
		></span>
		<span class="poll-label">live</span>
	</div>
</header>

<style>
	.cluster-bar {
		display: flex;
		align-items: center;
		gap: 16px;
		padding: 0 20px;
		height: 44px;
		background: var(--header-bg);
		border-bottom: 1px solid var(--border);
		flex-shrink: 0;
		font-family: var(--mono);
	}

	.bar-left {
		display: flex;
		align-items: center;
		gap: 6px;
		flex-shrink: 0;
	}

	.brand {
		font-size: 14px;
		font-weight: 700;
		letter-spacing: 0.06em;
		color: var(--text);
	}

	.sep {
		color: var(--dim);
		font-size: 13px;
	}

	.subtitle {
		font-size: 11px;
		color: var(--dim);
		letter-spacing: 0.04em;
	}

	.bar-center {
		display: flex;
		align-items: center;
		gap: 6px;
		flex: 1;
		flex-wrap: wrap;
		overflow: hidden;
	}

	.pill {
		display: flex;
		align-items: center;
		gap: 5px;
		padding: 3px 8px;
		border-radius: 3px;
		font-size: 11px;
		border: 1px solid var(--border);
		background: var(--surface);
		white-space: nowrap;
	}

	.pill.neutral {
		background: var(--surface);
		border-color: var(--border);
	}

	.pill.leader {
		background: rgba(55, 189, 141, 0.12);
		border-color: var(--success);
	}

	.pill.leader .dot {
		background: var(--success);
		box-shadow: 0 0 4px var(--success);
	}

	.pill.follower {
		background: rgba(255, 169, 90, 0.1);
		border-color: var(--accent);
	}

	.pill.follower .dot {
		background: var(--accent);
	}

	.pill.err {
		background: var(--error-bg);
		border-color: var(--error-border);
		color: var(--error);
	}

	.pill.err .dot {
		background: var(--error);
	}

	.pill.loading {
		background: var(--surface);
		border-color: var(--border);
		color: var(--dim);
	}

	.pill.warn {
		background: var(--warn-bg);
		border-color: var(--warn-border);
		color: var(--warn);
	}

	.dot {
		width: 6px;
		height: 6px;
		border-radius: 50%;
		background: var(--dim);
		flex-shrink: 0;
	}

	.pill-label {
		font-size: 9px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.pill-val {
		font-size: 11px;
		font-weight: 600;
		color: var(--text);
	}

	.bar-right {
		display: flex;
		align-items: center;
		gap: 5px;
		flex-shrink: 0;
	}

	.pulse {
		display: inline-block;
		width: 7px;
		height: 7px;
		border-radius: 50%;
		background: var(--success);
		box-shadow: 0 0 5px var(--success);
		animation: pulse 2s ease-in-out infinite;
	}

	.pulse.stale {
		background: var(--error);
		box-shadow: 0 0 5px var(--error);
		animation: none;
	}

	@keyframes pulse {
		0%,
		100% {
			opacity: 1;
		}
		50% {
			opacity: 0.3;
		}
	}

	.poll-label {
		font-size: 10px;
		font-weight: 600;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--dim);
	}

	.spinner {
		display: inline-block;
		width: 9px;
		height: 9px;
		border: 1.5px solid var(--dim);
		border-top-color: transparent;
		border-radius: 50%;
		animation: spin 0.6s linear infinite;
	}

	@keyframes spin {
		to {
			transform: rotate(360deg);
		}
	}
</style>
