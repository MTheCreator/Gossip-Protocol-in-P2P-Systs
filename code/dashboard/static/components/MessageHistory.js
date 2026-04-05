/**
 * MessageHistory.js — Animated message feed with gap visualization
 */
const MessageHistory = {
    name: 'MessageHistory',
    props: {
        messages: { type: Array, default: () => [] },
        lastDeliveredSeq: { type: Number, default: 0 },
    },
    template: `
        <div class="message-feed" ref="feed">
            <div v-if="entries.length === 0" class="empty-state" style="padding:24px">
                No messages yet
            </div>
            <div v-for="(entry, i) in entries" :key="entry.seq"
                 class="msg-item" :class="entry.type"
                 :style="{ animationDelay: Math.min(i * 25, 500) + 'ms' }">

                <div class="msg-seq-badge">{{ entry.seq }}</div>

                <div class="msg-body" v-if="entry.type === 'delivered'">
                    <div class="msg-content-text">{{ formatContent(entry.content) }}</div>
                    <div class="msg-meta-line">
                        {{ shortId(entry.original_sender || entry.sender_id) }}
                        &middot;
                        {{ formatTime(entry.timestamp) }}
                    </div>
                </div>

                <div class="msg-body" v-else>
                    <div class="msg-gap-label">Missing message</div>
                </div>
            </div>
        </div>
    `,

    computed: {
        entries() {
            if (!this.lastDeliveredSeq || this.lastDeliveredSeq <= 0) {
                return this.messages.map(m => ({
                    seq: m.seq_num,
                    type: 'delivered',
                    content: m.content,
                    sender_id: m.sender_id,
                    original_sender: m.original_sender,
                    timestamp: m.timestamp,
                }));
            }

            const delivered = {};
            this.messages.forEach(m => { delivered[m.seq_num] = m; });

            const result = [];
            const start = Math.max(1, this.lastDeliveredSeq - 99);
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

    watch: {
        entries() {
            this.$nextTick(() => {
                const feed = this.$refs.feed;
                if (feed) feed.scrollTop = feed.scrollHeight;
            });
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
                return new Date(ts * 1000).toLocaleTimeString();
            } catch {
                return String(ts);
            }
        },
        shortId(id) {
            if (!id) return '??';
            return id.substring(0, 8);
        },
    },
};
