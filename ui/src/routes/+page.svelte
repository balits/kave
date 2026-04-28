<script lang="ts">
	import Header from '$lib/components/header.svelte';
	import KvSection from '$lib/components/kv_section.svelte';
	import LeaseSection from '$lib/components/lease_section.svelte';
	import OtSection from '$lib/components/ot_section.svelte';
	import WatchSection from '$lib/components/watch_section.svelte';
	import { KaveClient } from '$lib/kave_client';
	import type { Component } from 'svelte';

	const url: string = import.meta.env.VITE_KAVE_CLUSTER_URL as string;
	const slotCount: number = Number(import.meta.env.VITE_KAVE_OT_SLOT_COUNT as unknown as number); // pls work ts gods
	const slotSize: number = Number(import.meta.env.VITE_KAVE_OT_SLOT_SIZE as unknown as number);

	const kvClient = $state(
		new KaveClient(url, {
			slotCount: slotCount,
			slotSize: slotSize
		})
	);

	type Section = {
		id: string;
		label: string;
		tag: string; // in sidebar
		description: string;
		component: Component<{ client: KaveClient }> | null;
		ready: boolean;
	};

	const sections: Section[] = [
		{
			id: 'kv',
			label: 'KV Explorer',
			tag: 'KV',
			description: 'KV API: put, get, range, delete with full MVCC metadata',
			component: KvSection,
			ready: true
		},
		{
			id: 'watch',
			label: 'Watch',
			tag: 'WS',
			description: 'Watch API: Live event stream over WebSocket',
			component: WatchSection,
			ready: true
		},
		{
			id: 'lease',
			label: 'Lease Lifecycle',
			tag: 'TTL',
			description: 'Lease API: grant, keep-alive, revoke with countdowns',
			component: LeaseSection,
			ready: true
		},
		{
			id: 'txn',
			label: 'Transactions',
			tag: 'TXN',
			description: 'KV Transactions: atomic CAS backed by MVCC revisions',
			component: null,
			ready: false
		},
		{
			id: 'ot',
			label: 'Oblivious Transfer',
			tag: 'OT',
			description: 'OT API: 1-out-of-N secret retrieval, where server learns nothing',
			component: OtSection,
			ready: true
		},
		{
			id: 'raft',
			label: 'Raft Cluster',
			tag: 'RAFT',
			description: 'Raft cluster view: node states, leader election, kill & recover',
			component: null,
			ready: false
		},
		{
			id: 'compact',
			label: 'Compaction',
			tag: 'GC',
			description: 'Manual compaction trigger and observe revision pruning',
			component: null,
			ready: false
		}
	];

	let activeId = $state('kv');
	const active = $derived(sections.find((s) => s.id === activeId)!);
	let ActiveComponent = $derived(active.component);
</script>

<div class="app">
	<Header client={kvClient} />

	<div class="body">
		<nav class="sidebar">
			{#each sections as s (s.id)}
				<button
					class="nav-item"
					class:active={activeId === s.id}
					class:pending={!s.ready}
					onclick={() => {
						if (s.ready) activeId = s.id;
					}}
					title={s.description}
				>
					<span class="nav-tag" class:active={activeId === s.id}>{s.tag}</span>
					<span class="nav-label">{s.label}</span>
					{#if !s.ready}
						<span class="nav-soon">soon</span>
					{/if}
				</button>
			{/each}
		</nav>

		<main class="content">
			<div class="content-header">
				<div class="content-title-row">
					<span class="content-tag">{active.tag}</span>
					<h1 class="content-title">{active.label}</h1>
				</div>
				<p class="content-desc">{active.description}</p>
			</div>

			<section class="content-body">
				{#if active.ready && active.component}
					<ActiveComponent client={kvClient}></ActiveComponent>
				{:else}
					<div class="placeholder">
						<span class="placeholder-tag">{active.tag}</span>
						<p class="placeholder-text">This section is not yet implemented.</p>
					</div>
				{/if}
			</section>
		</main>
	</div>
</div>
