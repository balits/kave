<script lang="ts">
    import { kvClient } from "./+state.svelte";
    import Kvexplorer from "../sections/kvexplorer.svelte";
</script>

<h1>Welcome to SvelteKit</h1>
<p>Visit <a href="https://svelte.dev/docs/kit">svelte.dev/docs/kit</a> to read the documentation</p>

<Kvexplorer client={kvClient}></Kvexplorer>
