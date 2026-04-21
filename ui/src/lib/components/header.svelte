<script lang="ts">
	import type { KaveClient } from '$lib/kave_client';
	import type { KaveStats } from '$lib/kv';
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
			<!-- node state -->
			<div class="pill" class:leader={isLeader} class:follower={!isLeader}>
				<span class="dot"></span>
				<span class="pill-label">state</span>
				<span class="pill-val">{stateLabel}</span>
			</div>

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
		<!-- dim/red if poll is stale -->
		<span
			class="pulse"
			class:stale={error}
			title={lastPoll ? `last poll ${Math.round((staleMs ?? 0) / 1000)}s ago` : 'never polled'}
		></span>
		<span class="poll-label">live</span>
	</div>
</header>
