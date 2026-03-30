/**
 * ChannelView.js — Channel detail panel with subscriber table and message history
 *
 * Props:
 *   channel — channel name
 *   detail  — object from /api/channels/{name}/detail
 *
 * Emits:
 *   close
 */
const ChannelView = {
    name: 'ChannelView',
    props: {
        channel: { type: String, required: true },
        detail: { type: Object, default: null },
    },
    emits: ['close'],
    template: `
        <div class="channel-detail">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
                <h3>Channel: {{ channel }}</h3>
                <button class="close-btn" @click="$emit('close')" style="position:static">&times;</button>
            </div>

            <div v-if="!detail" class="meta">Loading...</div>

            <template v-if="detail">
                <!-- Summary -->
                <div class="detail-section">
                    <div class="detail-row">
                        <span class="label">Sequencer</span>
                        <span>{{ detail.sequencer_address || 'Unknown' }}</span>
                    </div>
                    <div class="detail-row">
                        <span class="label">Core nodes</span>
                        <span>{{ (detail.core_addresses || []).length }}</span>
                    </div>
                    <div class="detail-row">
                        <span class="label">Total subscribers</span>
                        <span>{{ detail.total_subscribers }}</span>
                    </div>
                </div>

                <!-- Subscriber table -->
                <div class="detail-section">
                    <h4>Subscribers</h4>
                    <table class="subscriber-table">
                        <thead>
                            <tr>
                                <th>Node</th>
                                <th>Role</th>
                                <th>Last Seq</th>
                                <th>Pending</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="s in detail.subscribers" :key="s.node_id">
                                <td>{{ s.node_id.substring(0, 8) }}</td>
                                <td>
                                    <span v-if="s.is_sequencer" class="role-badge sequencer">SEQ</span>
                                    <span v-else-if="s.is_core" class="role-badge core">CORE</span>
                                    <span v-else class="meta">edge</span>
                                </td>
                                <td>{{ s.last_delivered_seq }}</td>
                                <td>{{ s.pending_count }}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>

                <!-- Message history -->
                <div class="detail-section" v-if="detail.delivered_messages && detail.delivered_messages.length">
                    <h4>Recent Messages</h4>
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
};
