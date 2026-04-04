/**
 * NodeDetail.js — Slide-in side panel for node details
 */
const NodeDetail = {
    name: 'NodeDetail',
    props: {
        node: { type: Object, required: true },
        channels: { type: Array, default: () => [] },
    },
    emits: ['close', 'show-messages'],
    template: `
        <div class="side-panel">
            <button class="panel-close" @click="$emit('close')">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="4" y1="4" x2="12" y2="12"/><line x1="12" y1="4" x2="4" y2="12"/>
                </svg>
            </button>

            <div class="panel-title">{{ node.label }}</div>
            <div class="panel-subtitle">{{ node.address }}</div>

            <!-- Status indicators -->
            <div style="display:flex;gap:6px;margin-bottom:18px;flex-wrap:wrap">
                <span class="role-badge" :class="node.status === 'alive' ? 'core' : 'sequencer'"
                      style="text-transform:capitalize">
                    {{ node.status }}
                </span>
                <span v-if="node.is_seed" class="role-badge" style="background:var(--blue-bg);color:var(--blue)">SEED</span>
                <template v-for="ch in node.channels" :key="'role-'+ch">
                    <span v-if="isSequencerFor(ch)" class="role-badge sequencer">SEQ:{{ ch }}</span>
                    <span v-else-if="isCoreFor(ch)" class="role-badge core">CORE:{{ ch }}</span>
                </template>
            </div>

            <!-- Identity -->
            <div class="detail-section">
                <div class="detail-section-title">Network</div>
                <div class="detail-row">
                    <span class="label">Node ID</span>
                    <span class="value" style="font-family:monospace;font-size:0.75rem">{{ node.id.substring(0, 16) }}...</span>
                </div>
                <div class="detail-row">
                    <span class="label">Neighbours</span>
                    <span class="value">{{ node.neighbour_count }}</span>
                </div>
            </div>

            <!-- Dissemination Stats -->
            <div class="detail-section" v-if="node.stats">
                <div class="detail-section-title">Dissemination</div>
                <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:4px">
                    <div style="text-align:center;padding:10px;background:var(--bg-alt);border-radius:var(--radius-sm)">
                        <div style="font-weight:700;font-size:1.1rem">{{ node.stats.published || 0 }}</div>
                        <div style="font-size:0.68rem;color:var(--text-muted)">Published</div>
                    </div>
                    <div style="text-align:center;padding:10px;background:var(--bg-alt);border-radius:var(--radius-sm)">
                        <div style="font-weight:700;font-size:1.1rem">{{ node.stats.forwarded || 0 }}</div>
                        <div style="font-size:0.68rem;color:var(--text-muted)">Forwarded</div>
                    </div>
                    <div style="text-align:center;padding:10px;background:var(--bg-alt);border-radius:var(--radius-sm)">
                        <div style="font-weight:700;font-size:1.1rem">{{ node.stats.delivered || 0 }}</div>
                        <div style="font-size:0.68rem;color:var(--text-muted)">Delivered</div>
                    </div>
                </div>
            </div>

            <!-- Channels -->
            <div class="detail-section" v-if="node.channels && node.channels.length">
                <div class="detail-section-title">Channels</div>
                <div v-for="ch in node.channels" :key="ch" class="channel-tag">
                    <div class="channel-tag-name">
                        <span style="color:var(--primary);font-weight:700">#</span>
                        {{ ch }}
                        <span v-if="isSequencerFor(ch)" class="role-badge sequencer" style="font-size:0.6rem">SEQ</span>
                        <span v-else-if="isCoreFor(ch)" class="role-badge core" style="font-size:0.6rem">CORE</span>
                    </div>
                    <button class="btn-outline" @click="$emit('show-messages', { nodeId: node.id, channel: ch })">
                        Messages
                    </button>
                </div>
            </div>

            <!-- Delivery Buffers -->
            <div class="detail-section" v-if="node.delivery_buffers && Object.keys(node.delivery_buffers).length">
                <div class="detail-section-title">Delivery Buffers</div>
                <div v-for="(buf, ch) in node.delivery_buffers" :key="ch" class="detail-row">
                    <span class="label">{{ ch }}</span>
                    <span class="value" style="font-family:monospace;font-size:0.78rem">
                        seq={{ buf.last_delivered_seq }} &middot; {{ buf.pending_count }} pending
                    </span>
                </div>
            </div>
        </div>
    `,

    methods: {
        isCoreFor(channel) {
            const cs = this.node.core_status || {};
            return cs[channel] && cs[channel].is_core;
        },
        isSequencerFor(channel) {
            const ss = this.node.sequencer_status || {};
            return ss[channel] && ss[channel].is_sequencer;
        },
    },
};
