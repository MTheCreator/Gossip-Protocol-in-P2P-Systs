/**
 * ChannelList.js — Grid of channel cards
 *
 * Props:
 *   channels — array from /api/channels
 *
 * Emits:
 *   select(channelName) — when a card is clicked
 */
const ChannelList = {
    name: 'ChannelList',
    props: {
        channels: { type: Array, default: () => [] },
    },
    emits: ['select'],
    template: `
        <div class="card-grid">
            <div v-for="ch in channels" :key="ch.name"
                 class="card" @click="$emit('select', ch.name)">
                <h3>{{ ch.name }}</h3>
                <div class="stat">
                    <span class="label">Subscribers</span>
                    <span>{{ ch.total_subscribers }}</span>
                </div>
                <div class="stat" v-if="ch.delivery_stats">
                    <span class="label">Reporting nodes</span>
                    <span>{{ Object.keys(ch.delivery_stats).length }}</span>
                </div>
                <div class="meta" style="margin-top:8px">Click for details</div>
            </div>
            <div v-if="channels.length === 0" class="meta" style="padding:24px">
                No channels discovered yet.
            </div>
        </div>
    `,
};
