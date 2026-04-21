import { KaveClient } from "$lib/kave_client";

const url: string = process.env.KAVE_CLUSTER_URL as string
const slotCount: number = process.env.KAVE_OT_SLOT_COUNT as unknown as number
const slotSize: number = process.env.KAVE_OT_SLOT_SIZE as unknown as number

export const kvClient = $state(new KaveClient(url, {
        slotCount: slotCount,
        slotSize: slotSize,
    }
))