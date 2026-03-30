/**
 * MessageHistory.js — Scrollable message history with gap visualization
 *
 * Props:
 *   messages         — array of { seq_num, content, sender_id, timestamp, ... }
 *   lastDeliveredSeq — highest delivered seq (to render gaps up to)
 */
const MessageHistory = {
    name: 'MessageHistory',
    props: {
        messages: { type: Array, default: () => [] },
        lastDeliveredSeq: { type: Number, default: 0 },
    },
    template: `
        <div class="message-history">
            <div v-if="entries.length === 0" class="meta" style="padding:12px">
                No messages yet.
            </div>
            <div v-for="entry in entries" :key="entry.seq"
                 class="msg-card" :class="entry.type">
                <template v-if="entry.type === 'delivered'">
                    <span class="msg-seq">[{{ entry.seq }}]</span>
                    <span class="msg-content">{{ formatContent(entry.content) }}</span>
                    <div class="msg-meta">
                        from {{ (entry.original_sender || entry.sender_id || '').substring(0, 8) }}
                        &middot; {{ formatTime(entry.timestamp) }}
                    </div>
                </template>
                <template v-else>
                    Missing message (seq={{ entry.seq }})
                </template>
            </div>
        </div>
    `,

    computed: {
        entries() {
            if (!this.lastDeliveredSeq || this.lastDeliveredSeq <= 0) {
                // Just show delivered messages in order
                return this.messages.map(m => ({
                    seq: m.seq_num,
                    type: 'delivered',
                    content: m.content,
                    sender_id: m.sender_id,
                    original_sender: m.original_sender,
                    timestamp: m.timestamp,
                }));
            }

            // Build a lookup of delivered seq_nums
            const delivered = {};
            this.messages.forEach(m => {
                delivered[m.seq_num] = m;
            });

            // Iterate 1..lastDeliveredSeq, mark gaps
            const result = [];
            const start = Math.max(1, this.lastDeliveredSeq - 99); // show last 100
            for (let seq = start; seq <= this.lastDeliveredSeq; seq++) {
                const m = delivered[seq];
                if (m) {
                    result.push({
                        seq,
                        type: 'delivered',
                        content: m.content,
                        sender_id: m.sender_id,
                        original_sender: m.original_sender,
                        timestamp: m.timestamp,
                    });
                } else {
                    result.push({ seq, type: 'gap' });
                }
            }
            return result;
        },
    },

    methods: {
        formatContent(content) {
            if (content === null || content === undefined) return '';
            if (typeof content === 'object') return JSON.stringify(content);
            return String(content);
        },
        formatTime(ts) {
            if (!ts) return '';
            try {
                const d = new Date(ts * 1000);
                return d.toLocaleTimeString();
            } catch {
                return String(ts);
            }
        },
    },
};
