/**
 * NodeDetail.js — Side panel showing node details
 *
 * Props:
 *   node     — node object from topology
 *   channels — array of channel objects
 *
 * Emits:
 *   close
 *   show-messages({ nodeId, channel })
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
            <button class="close-btn" @click="$emit('close')">&times;</button>
            <h2>{{ node.label }}</h2>

            <!-- Identity -->
            <div class="detail-section">
                <h4>Identity</h4>
                <div class="detail-row">
                    <span class="label">Address</span>
                    <span>{{ node.address }}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Status</span>
                    <span>{{ node.status }}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Seed</span>
                    <span>{{ node.is_seed ? 'Yes' : 'No' }}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Neighbours</span>
                    <span>{{ node.neighbour_count }}</span>
                </div>
            </div>

            <!-- Stats -->
            <div class="detail-section" v-if="node.stats">
                <h4>Dissemination</h4>
                <div class="detail-row">
                    <span class="label">Published</span>
                    <span>{{ node.stats.published || 0 }}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Forwarded</span>
                    <span>{{ node.stats.forwarded || 0 }}</span>
                </div>
                <div class="detail-row">
                    <span class="label">Delivered</span>
                    <span>{{ node.stats.delivered || 0 }}</span>
                </div>
            </div>

            <!-- Channels with roles -->
            <div class="detail-section" v-if="node.channels && node.channels.length">
                <h4>Channels</h4>
                <div v-for="ch in node.channels" :key="ch"
                     style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;font-size:0.85rem">
                    <span>
                        {{ ch }}
                        <span v-if="isSequencerFor(ch)" class="role-badge sequencer" style="margin-left:4px">SEQ</span>
                        <span v-else-if="isCoreFor(ch)" class="role-badge core" style="margin-left:4px">CORE</span>
                    </span>
                    <button class="btn-sm" @click="$emit('show-messages', { nodeId: node.id, channel: ch })">
                        Messages
                    </button>
                </div>
            </div>

            <!-- Delivery buffers -->
            <div class="detail-section" v-if="node.delivery_buffers && Object.keys(node.delivery_buffers).length">
                <h4>Delivery Buffers</h4>
                <div v-for="(buf, ch) in node.delivery_buffers" :key="ch" class="detail-row">
                    <span class="label">{{ ch }}</span>
                    <span>seq={{ buf.last_delivered_seq }}, pending={{ buf.pending_count }}</span>
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
