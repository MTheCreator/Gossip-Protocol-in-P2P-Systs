/**
 * ChannelList.js — Animated channel card grid
 */
const ChannelList = {
    name: 'ChannelList',
    props: {
        channels: { type: Array, default: () => [] },
    },
    emits: ['select'],
    template: `
        <div class="card-grid">
            <div v-for="(ch, i) in channels" :key="ch.name"
                 class="channel-card"
                 :style="{ animationDelay: (i * 60) + 'ms' }"
                 @click="$emit('select', ch.name)">
                <div class="channel-card-name">
                    <span class="hash">#</span>
                    {{ ch.name }}
                </div>
                <div class="channel-card-stats">
                    <div class="channel-stat">
                        <span class="channel-stat-value">{{ ch.total_subscribers }}</span>
                        <span class="channel-stat-label">Subscribers</span>
                    </div>
                    <div class="channel-stat" v-if="ch.delivery_stats">
                        <span class="channel-stat-value">{{ Object.keys(ch.delivery_stats).length }}</span>
                        <span class="channel-stat-label">Reporting</span>
                    </div>
                </div>
                <div class="channel-card-footer">
                    <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.5">
                        <path d="M5 3l4 4-4 4" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                    View details
                </div>
            </div>
            <div v-if="channels.length === 0" class="empty-state">
                <svg width="40" height="40" viewBox="0 0 40 40" fill="none" stroke="#94a3b8" stroke-width="1.5" style="margin-bottom:12px">
                    <circle cx="20" cy="20" r="16"/>
                    <path d="M14 20h12M20 14v12" stroke-linecap="round"/>
                </svg>
                <div>No channels discovered yet</div>
                <div style="font-size:0.78rem;margin-top:4px;color:#94a3b8">Channels appear when nodes subscribe and report status</div>
            </div>
        </div>
    `,
};
