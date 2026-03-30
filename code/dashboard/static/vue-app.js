/**
 * vue-app.js — Main Vue 3 application for Gossip Protocol Dashboard
 *
 * Polls the REST API every 2 seconds and feeds data into components.
 */
const { createApp } = Vue;

const app = createApp({
    data() {
        return {
            activeTab: 'graph',
            connected: false,
            topology: { nodes: [], edges: [] },
            channels: [],
            selectedChannel: '',        // for graph overlay
            channelDetail: null,
            selectedNode: null,          // for side panel
            selectedChannelForView: null, // for channel detail view
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

    mounted() {
        this.poll();
        this._pollTimer = setInterval(() => this.poll(), 2000);
    },

    beforeUnmount() {
        if (this._pollTimer) clearInterval(this._pollTimer);
    },

    methods: {
        async poll() {
            try {
                const [topoRes, chRes] = await Promise.all([
                    fetch('/api/topology'),
                    fetch('/api/channels'),
                ]);
                if (topoRes.ok) {
                    this.topology = await topoRes.json();
                    this.connected = true;
                }
                if (chRes.ok) {
                    this.channels = await chRes.json();
                }

                // Refresh channel detail if one is selected
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
