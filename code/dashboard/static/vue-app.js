/**
 * vue-app.js — Main Vue 3 application
 *
 * Features: overview tab with KPIs, topology graph with overlays,
 * channel detail, node detail, message modal. Polls API every 2s.
 */
const { createApp } = Vue;

const app = createApp({
    data() {
        return {
            appReady: false,
            activeTab: 'overview',
            connected: false,
            topology: { nodes: [], edges: [] },
            trafficEvents: [],
            lastTrafficId: 0,
            channels: [],
            selectedChannel: '',
            channelDetail: null,
            selectedNode: null,
            selectedChannelForView: null,
            messageModal: {
                visible: false,
                nodeId: '',
                channel: '',
                messages: [],
                lastDeliveredSeq: 0,
            },
            _pollTimer: null,
        };
    },

    computed: {
        kpis() {
            const nodes = this.topology.nodes;
            const alive = nodes.filter(n => n.status === 'alive').length;
            const totalDelivered = nodes.reduce((sum, n) => sum + ((n.stats || {}).delivered || 0), 0);

            // Count sequencers and core nodes across all channels
            let seqCount = 0;
            let coreCount = 0;
            nodes.forEach(n => {
                const ss = n.sequencer_status || {};
                const cs = n.core_status || {};
                for (const ch in ss) { if (ss[ch].is_sequencer) seqCount++; }
                for (const ch in cs) { if (cs[ch].is_core) coreCount++; }
            });

            return [
                {
                    label: 'Active Nodes',
                    value: alive,
                    bg: 'var(--green-bg)',
                    icon: '<svg width="22" height="22" viewBox="0 0 22 22" fill="none"><circle cx="11" cy="11" r="8" stroke="#10b981" stroke-width="2"/><path d="M7.5 11l2.5 2.5 4.5-5" stroke="#10b981" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
                },
                {
                    label: 'Channels',
                    value: this.channels.length,
                    bg: 'var(--primary-bg)',
                    icon: '<svg width="22" height="22" viewBox="0 0 22 22" fill="none"><path d="M4 7h14M4 11h14M4 15h10" stroke="#6366f1" stroke-width="2" stroke-linecap="round"/></svg>',
                },
                {
                    label: 'Core Nodes',
                    value: coreCount,
                    bg: 'var(--amber-bg)',
                    icon: '<svg width="22" height="22" viewBox="0 0 22 22" fill="none"><path d="M11 3l7 4v8l-7 4-7-4V7l7-4z" stroke="#f59e0b" stroke-width="2" stroke-linejoin="round"/></svg>',
                },
                {
                    label: 'Msgs Delivered',
                    value: totalDelivered,
                    bg: 'var(--blue-bg)',
                    icon: '<svg width="22" height="22" viewBox="0 0 22 22" fill="none"><path d="M4 16V6l7 5 7-5v10" stroke="#3b82f6" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
                },
            ];
        },
    },

    mounted() {
        document.body.classList.add('dashboard-ready');
        const appRoot = document.getElementById('app');
        if (appRoot) appRoot.classList.add('loaded');
        this.poll();
        this._pollTimer = setInterval(() => this.poll(), 2000);
        // Trigger entrance animation
        requestAnimationFrame(() => { this.appReady = true; });
    },

    beforeUnmount() {
        if (this._pollTimer) clearInterval(this._pollTimer);
    },

    methods: {
        async poll() {
            try {
                const [topoRes, chRes, trafficRes] = await Promise.all([
                    fetch('/api/topology'),
                    fetch('/api/channels'),
                    fetch(`/api/traffic?since_id=${encodeURIComponent(this.lastTrafficId)}`),
                ]);
                if (topoRes.ok) {
                    this.topology = await topoRes.json();
                    this.connected = true;
                }
                if (chRes.ok) {
                    this.channels = await chRes.json();
                }
                if (trafficRes.ok) {
                    const payload = await trafficRes.json();
                    const fresh = Array.isArray(payload)
                        ? payload
                        : (Array.isArray(payload.events) ? payload.events : []);
                    const latest = Array.isArray(payload)
                        ? 0
                        : (typeof payload.latest_id === 'number' ? payload.latest_id : 0);

                    if (latest > this.lastTrafficId) {
                        this.lastTrafficId = latest;
                    } else if (fresh.length > 0) {
                        const maxSeen = Math.max(
                            this.lastTrafficId,
                            ...fresh.map(ev => (typeof ev.id === 'number' ? ev.id : 0)),
                        );
                        this.lastTrafficId = maxSeen;
                    }

                    if (fresh.length > 0) {
                        const arrivalMs = Date.now();
                        const enriched = fresh.map(ev => ({
                            ...ev,
                            arrival_ms: arrivalMs,
                        }));
                        this.trafficEvents = this.trafficEvents.concat(enriched);
                    }

                    const cutoffSec = (Date.now() / 1000) - 15;
                    this.trafficEvents = this.trafficEvents.filter(ev => {
                        const ts = typeof ev.ts === 'number' ? ev.ts : Number(ev.ts);
                        return Number.isFinite(ts) ? ts >= cutoffSec : true;
                    });
                    if (this.trafficEvents.length > 6000) {
                        this.trafficEvents = this.trafficEvents.slice(-4000);
                    }
                }

                if (this.selectedChannel) {
                    this.fetchChannelDetail(this.selectedChannel);
                }
                if (this.selectedChannelForView) {
                    this.fetchChannelDetail(this.selectedChannelForView);
                }
            } catch (e) {
                this.connected = false;
            }
        },

        async fetchChannelDetail(name) {
            try {
                const res = await fetch(`/api/channels/${encodeURIComponent(name)}/detail`);
                if (res.ok) {
                    this.channelDetail = await res.json();
                }
            } catch (e) {
                // ignore
            }
        },

        onChannelOverlayChange() {
            if (this.selectedChannel) {
                this.fetchChannelDetail(this.selectedChannel);
            } else {
                this.channelDetail = null;
            }
        },

        onNodeClick(node) {
            this.selectedNode = node;
            if (this.activeTab === 'overview') {
                this.activeTab = 'graph';
            }
        },

        onChannelSelect(name) {
            this.selectedChannelForView = name;
            this.fetchChannelDetail(name);
        },

        async showNodeMessages({ nodeId, channel }) {
            this.messageModal.nodeId = nodeId;
            this.messageModal.channel = channel;
            this.messageModal.messages = [];
            this.messageModal.lastDeliveredSeq = 0;
            this.messageModal.visible = true;

            try {
                const res = await fetch(
                    `/api/nodes/${encodeURIComponent(nodeId)}/messages/${encodeURIComponent(channel)}`
                );
                if (res.ok) {
                    const msgs = await res.json();
                    this.messageModal.messages = msgs;
                    this.messageModal.lastDeliveredSeq = msgs.length > 0
                        ? Math.max(...msgs.map(m => m.seq_num || 0))
                        : 0;
                }
            } catch (e) {
                // ignore
            }
        },
    },
});

// Register components
app.component('network-graph', NetworkGraph);
app.component('channel-list', ChannelList);
app.component('channel-view', ChannelView);
app.component('node-detail', NodeDetail);
app.component('message-history', MessageHistory);

app.mount('#app');
