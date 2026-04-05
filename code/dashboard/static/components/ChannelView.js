/**
 * ChannelView.js — Channel detail with subscriber table and messages
 */
const ChannelView = {
    name: 'ChannelView',
    props: {
        channel: { type: String, required: true },
        detail: { type: Object, default: null },
    },
    emits: ['close'],
    template: `
        <div class="channel-detail-card">
            <div class="channel-detail-header">
                <h3><span style="color:var(--primary);font-weight:700">#</span> {{ channel }}</h3>
                <button class="icon-btn" @click="$emit('close')">
                    <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" stroke-width="2">
                        <line x1="5" y1="5" x2="13" y2="13"/><line x1="13" y1="5" x2="5" y2="13"/>
                    </svg>
                </button>
            </div>

            <div v-if="!detail" class="empty-state" style="padding:32px">
                <div style="color:var(--text-muted)">Loading channel data...</div>
            </div>

            <template v-if="detail">
                <!-- Summary strip -->
                <div class="channel-summary">
                    <div class="summary-item">
                        <span class="summary-value">{{ detail.total_subscribers }}</span>
                        <span class="summary-label">Subscribers</span>
                    </div>
                    <div class="summary-item">
                        <span class="summary-value">{{ (detail.core_addresses || []).length }}</span>
                        <span class="summary-label">Core Nodes</span>
                    </div>
                    <div class="summary-item">
                        <span class="summary-value" style="font-family:monospace;font-size:0.85rem">{{ shortAddr(detail.sequencer_address) }}</span>
                        <span class="summary-label">Sequencer</span>
                    </div>
                </div>

                <!-- Subscriber table -->
                <table class="subscriber-table" v-if="detail.subscribers && detail.subscribers.length">
                    <thead>
                        <tr>
                            <th>Node</th>
                            <th>Role</th>
                            <th>Last Seq</th>
                            <th>Pending</th>
                            <th>Election</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr v-for="s in detail.subscribers" :key="s.node_id">
                            <td class="node-id">{{ s.node_id.substring(0, 10) }}</td>
                            <td>
                                <span v-if="s.is_sequencer" class="role-badge sequencer">SEQUENCER</span>
                                <span v-else-if="s.is_core" class="role-badge core">CORE</span>
                                <span v-else class="role-badge edge-role">EDGE</span>
                            </td>
                            <td>{{ s.last_delivered_seq }}</td>
                            <td>
                                <span :style="{ color: s.pending_count > 0 ? 'var(--amber)' : 'var(--green)' }">
                                    {{ s.pending_count }}
                                </span>
                            </td>
                            <td>
                                <span v-if="s.election_in_progress" style="color:var(--red);font-weight:600;font-size:0.75rem">VOTING</span>
                                <span v-else style="color:var(--text-muted);font-size:0.75rem">--</span>
                            </td>
                        </tr>
                    </tbody>
                </table>

                <!-- Messages section -->
                <div v-if="detail.delivered_messages && detail.delivered_messages.length" style="padding:16px 24px">
                    <div style="font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;color:var(--text-muted);margin-bottom:10px">
                        Recent Messages
                    </div>
                    <message-history
                        :messages="detail.delivered_messages"
                        :last-delivered-seq="maxSeq">
                    </message-history>
                </div>
            </template>
        </div>
    `,

    computed: {
        maxSeq() {
            if (!this.detail || !this.detail.delivered_messages) return 0;
            return Math.max(0, ...this.detail.delivered_messages.map(m => m.seq_num || 0));
        },
    },

    methods: {
        shortAddr(addr) {
            if (!addr) return 'Unknown';
            return addr;
        },
    },
};
