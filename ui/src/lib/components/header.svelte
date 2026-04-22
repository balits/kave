<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { KaveStats } from '$lib/kv';
	import type { KavePeer } from '$lib/kv';
	import { onDestroy } from 'svelte';

	let { client }: { client: KaveClient } = $props();

	let stats = $state<KaveStats | null>(null);
	let error = $state(false);
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

	const currentRev: number = $derived(stats?.revision ?? -1)
	const compactedRev: number = $derived(stats?.compacted_revision ?? -1)
	const stateLabel: string = $derived(stats?.state ?? '—');
	const isLeader: boolean = $derived(stateLabel === 'Leader');
	const term: number = $derived(stats?.term ?? -1);
	const commitIdx: number = $derived(stats?.commit_index ?? -1);
	const appliedIdx: number = $derived(stats?.applied_index ?? -1);
	const leaderId: string = $derived(stats?.leader_id ?? '');
	const fsmPending: number = $derived(stats?.fsm_pending ?? -1);
	const peers: KavePeer[] = $derived.by(() => {
		const raw = stats?.readable_configuration ?? '[]';
		try {
			return JSON.parse(raw) as KavePeer[];
		} catch {
			return [];
		}
	});

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
			<div class="pill err">
				<span class="dot"></span>
				<span>cluster unreachable</span>
			</div>
		{:else if !stats}
			<div class="pill loading">
				<span class="spinner"></span>
				<span>connecting…</span>
			</div>
		{:else}
			<div class="pill neutral">
				<span class="pill-label">current revision</span>
				<span class="pill-val">{currentRev}</span>
			</div>

			<div class="pill neutral">
				<span class="pill-label">compacted revision</span>
				<span class="pill-val">{compactedRev}</span>
			</div>

			<div class="pill neutral">
				<span class="pill-label">term</span>
				<span class="pill-val">{term}</span>
			</div>

			<div class="pill neutral">
				<span class="pill-label">commit&nbsp;index</span>
				<span class="pill-val">{commitIdx}</span>
			</div>

			<div class="pill neutral">
				<span class="pill-label">applied&nbsp;index</span>
				<span class="pill-val">{appliedIdx}</span>
			</div>

			{#if fsmPending > 0}
				<div class="pill warn">
					<span class="pill-label">fsm lag</span>
					<span class="pill-val">{fsmPending}</span>
				</div>
			{/if}

			<!-- divider between status pills and peer list -->
			{#if peers.length > 0}
				<div class="bar-divider"></div>

				{#each peers as p (p.id)}
					{@const isNodeLeader = p.id === leaderId}
					{@const isVoter = p.suffrage === 'Voter'}
					<div
						class="node-chip"
						class:node-leader={isNodeLeader}
						class:node-nonvoter={!isVoter}
						title={`id: ${p.id}\naddr: ${p.address}\nsuffrage: ${p.suffrage}`}
					>
						<span class="node-dot" class:leader-dot={isNodeLeader}></span>
						<span class="node-id">{p.id}</span>
						<!-- <span class="node-addr">{p.address}</span> -->
						{#if !isVoter}
							<span class="node-badge">{p.suffrage}</span>
						{/if}
					</div>
				{/each}
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
		gap: 12px;
		padding: 0 20px;
		height: 44px;
		background: var(--header-bg);
		border-bottom: 1px solid var(--border);
		flex-shrink: 0;
		font-family: var(--mono);
		overflow: hidden;
	}

	/* ── brand ── */
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

	/* ── center ── */
	.bar-center {
		display: flex;
		align-items: center;
		gap: 6px;
		flex: 1;
		overflow: hidden;
		flex-wrap: nowrap;
	}

	/* status pills */
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
		flex-shrink: 0;
	}

	.pill.neutral {
		background: var(--surface);
		border-color: var(--border);
	}
	.pill.leader {
		background: rgba(55, 189, 141, 0.12);
		border-color: var(--success);
	}
	.pill.follower {
		background: rgba(255, 169, 90, 0.1);
		border-color: var(--accent);
	}
	.pill.err {
		background: var(--error-bg);
		color: var(--error);
		border-color: var(--error-border);
	}
	.pill.loading {
		background: var(--surface);
		color: var(--dim);
		border-color: var(--border);
	}
	.pill.warn {
		background: var(--warn-bg);
		color: var(--warn);
		border-color: var(--warn-border);
	}

	.pill.leader .dot {
		background: var(--success);
		box-shadow: 0 0 4px var(--success);
	}
	.pill.follower .dot {
		background: var(--accent);
	}
	.pill.err .dot {
		background: var(--error);
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

	.bar-divider {
		width: 1px;
		height: 20px;
		background: var(--border);
		flex-shrink: 0;
		margin: 0 2px;
	}

	.node-chip {
		display: flex;
		align-items: center;
		gap: 5px;
		padding: 3px 8px 3px 6px;
		border-radius: 3px;
		font-size: 11px;
		border: 1px solid var(--border);
		background: var(--surface);
		white-space: nowrap;
		flex-shrink: 0;
		cursor: default;
	}

	.node-chip.node-leader {
		background: rgba(55, 189, 141, 0.12);
		border-color: var(--success);
	}

	/* non-voters (staging, removed) are visually different than voters */
	.node-chip.node-nonvoter {
		opacity: 0.5;
	}

	.node-dot {
		width: 6px;
		height: 6px;
		border-radius: 50%;
		background: var(--dim);
		flex-shrink: 0;
	}

	.node-dot.leader-dot {
		background: var(--success);
		box-shadow: 0 0 4px var(--success);
		animation: pulse 2s ease-in-out infinite;
	}

	.node-id {
		font-weight: 600;
		color: var(--text);
		font-size: 11px;
	}

	.node-addr {
		font-size: 10px;
		color: var(--dim);
	}

	.node-badge {
		font-size: 9px;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		background: var(--warn-bg);
		color: var(--warn);
		border: 1px solid var(--warn-border);
		border-radius: 2px;
		padding: 0 4px;
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
			opacity: 0.35;
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
