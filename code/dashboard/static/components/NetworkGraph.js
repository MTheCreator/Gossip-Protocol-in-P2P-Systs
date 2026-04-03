/**
 * NetworkGraph.js — Native SVG topology renderer (no external graph library).
 */
const NetworkGraph = {
    name: 'NetworkGraph',
    props: {
        topology: { type: Object, default: () => ({ nodes: [], edges: [] }) },
        selectedChannel: { type: String, default: '' },
        channelDetail: { type: Object, default: null },
        mini: { type: Boolean, default: false },
    },
    emits: ['node-click'],
    template: `
        <div class="network-graph-shell">
            <div v-if="!mini" class="graph-overlay-toolbar">
                <button class="graph-tool-btn" type="button" @click="zoomOut">-</button>
                <button class="graph-tool-btn" type="button" @click="resetView">Reset</button>
                <button class="graph-tool-btn" type="button" @click="zoomIn">+</button>
            </div>

            <div class="graph-status-bar" v-if="!mini">
                <span class="graph-status-chip">{{ graphNodes.length }} nodes</span>
                <span class="graph-status-chip">{{ graphEdges.length }} edges</span>
                <span v-if="selectedChannel" class="graph-status-chip graph-status-chip-accent">#{{ selectedChannel }}</span>
            </div>

            <div class="graph-canvas-wrap">
                <div v-if="graphNodes.length === 0" class="graph-empty-state">
                    <div class="graph-empty-title">No topology data yet</div>
                    <div class="graph-empty-text">Start the monitor and let nodes report status to populate the network view.</div>
                </div>

                <svg
                    v-else
                    class="graph-fallback-svg"
                    viewBox="0 0 1200 800"
                    preserveAspectRatio="xMidYMid meet"
                    @wheel.prevent="onWheel"
                    @mousedown="startPan"
                    @mousemove="onPan"
                    @mouseup="endPan"
                    @mouseleave="endPan"
                >
                    <defs>
                        <linearGradient id="edgeGrad" x1="0%" y1="0%" x2="100%" y2="0%">
                            <stop offset="0%" stop-color="#6366f1" stop-opacity="0.45" />
                            <stop offset="100%" stop-color="#10b981" stop-opacity="0.35" />
                        </linearGradient>
                    </defs>

                    <g :transform="viewTransform">
                        <line
                            v-for="edge in graphEdges"
                            :key="edge.id"
                            :x1="edge.x1"
                            :y1="edge.y1"
                            :x2="edge.x2"
                            :y2="edge.y2"
                            :stroke="edge.stroke"
                            :stroke-width="edge.width"
                            :stroke-opacity="edge.opacity"
                            :stroke-dasharray="edge.dashArray"
                            :stroke-dashoffset="edge.dashOffset"
                            class="graph-edge"
                            stroke-linecap="round"
                        />

                        <g
                            v-for="node in graphNodes"
                            :key="node.id"
                            :transform="'translate(' + node.x + ', ' + node.y + ')'"
                            class="graph-node"
                            :style="{ animationDelay: node.animDelay }"
                            @click="$emit('node-click', node.raw)"
                        >
                            <circle
                                v-if="node.isSeed"
                                :r="node.radius + 10"
                                fill="none"
                                stroke="rgba(99, 102, 241, 0.2)"
                                stroke-width="4"
                            />
                            <circle
                                :r="node.radius"
                                :fill="node.fill"
                                :stroke="node.stroke"
                                stroke-width="3"
                                :opacity="node.opacity"
                            />
                            <text y="-18" text-anchor="middle" fill="#334155" font-size="12" font-family="Inter, system-ui, sans-serif">{{ node.label }}</text>
                            <text y="30" text-anchor="middle" fill="#64748b" font-size="10" font-family="Inter, system-ui, sans-serif">{{ node.status }}</text>
                        </g>
                    </g>
                </svg>
            </div>

            <div v-if="!mini" class="graph-hint">Scroll to zoom. Drag to pan. Click node for details.</div>
        </div>
    `,

    data() {
        return {
            scale: 1,
            panX: 0,
            panY: 0,
            panning: false,
            lastMouseX: 0,
            lastMouseY: 0,
            animPhase: 0,
            animTimer: null,
        };
    },

    mounted() {
        this.startAnimation();
    },

    beforeUnmount() {
        this.stopAnimation();
    },

    computed: {
        channelNodeSet() {
            const set = new Set();
            if (this.selectedChannel && this.channelDetail && Array.isArray(this.channelDetail.subscribers)) {
                this.channelDetail.subscribers.forEach(sub => set.add(sub.node_id));
            }
            return set;
        },

        graphNodes() {
            const nodes = this.topology.nodes || [];
            if (nodes.length === 0) return [];

            const cols = Math.max(2, Math.ceil(Math.sqrt(nodes.length)));
            const rows = Math.max(1, Math.ceil(nodes.length / cols));
            const xGap = 980 / Math.max(1, cols - 1);
            const yGap = 620 / Math.max(1, rows - 1);

            return nodes.map((node, index) => {
                const row = Math.floor(index / cols);
                const col = index % cols;
                const baseX = 110 + col * xGap;
                const baseY = 90 + row * yGap;
                const motion = this.getNodeMotion(node.id, index);
                const x = baseX + motion.dx;
                const y = baseY + motion.dy;
                const inChannel = this.channelNodeSet.size === 0 || this.channelNodeSet.has(node.id);

                return {
                    id: node.id,
                    raw: node,
                    x,
                    y,
                    label: node.label || node.id.substring(0, 8),
                    status: node.status || 'unknown',
                    isSeed: !!node.is_seed,
                    radius: this.getRadius(node, inChannel),
                    fill: this.getFill(node, inChannel),
                    stroke: this.getStroke(node, inChannel),
                    opacity: inChannel ? 0.98 : 0.2,
                    animDelay: `${(index % 7) * 0.18}s`,
                };
            });
        },

        graphEdges() {
            const lookup = new Map(this.graphNodes.map(node => [node.id, node]));
            const hasOverlay = this.channelNodeSet.size > 0;

            return (this.topology.edges || []).map((edge, index) => {
                const from = lookup.get(edge.source);
                const to = lookup.get(edge.target);
                const inOverlay = !hasOverlay || (this.channelNodeSet.has(edge.source) && this.channelNodeSet.has(edge.target));
                const probability = typeof edge.probability === 'number' ? edge.probability : 1;

                return {
                    id: edge.id || `${edge.source}-${edge.target}-${index}`,
                    x1: from ? from.x : 0,
                    y1: from ? from.y : 0,
                    x2: to ? to.x : 0,
                    y2: to ? to.y : 0,
                    stroke: edge.status === 'dead' || probability < 0.3 ? '#ef4444' : 'url(#edgeGrad)',
                    width: 1 + probability * 2.2,
                    opacity: inOverlay ? 0.8 : 0.12,
                    dashArray: inOverlay ? '9 7' : '5 9',
                    dashOffset: -((this.animPhase * 42) + (index * 13)) % 200,
                };
            });
        },

        viewTransform() {
            return `translate(${this.panX} ${this.panY}) scale(${this.scale})`;
        },
    },

    methods: {
        getRadius(node, inChannel) {
            if (!inChannel) return this.mini ? 8 : 10;
            if (node.is_seed) return this.mini ? 16 : 20;
            if (node.status === 'suspect') return this.mini ? 13 : 16;
            if (node.status === 'dead') return this.mini ? 9 : 11;
            return this.mini ? 11 : 14;
        },

        getFill(node, inChannel) {
            if (!inChannel) return '#cbd5e1';
            if (node.is_seed) return '#6366f1';
            if (node.status === 'dead') return '#cbd5e1';
            if (node.status === 'suspect') return '#facc15';
            return '#10b981';
        },

        getStroke(node, inChannel) {
            if (!inChannel) return '#94a3b8';
            if (node.is_seed) return '#4338ca';
            if (node.status === 'dead') return '#94a3b8';
            if (node.status === 'suspect') return '#ca8a04';
            return '#059669';
        },

        onWheel(event) {
            const delta = event.deltaY > 0 ? -0.08 : 0.08;
            this.scale = Math.min(2.2, Math.max(0.55, this.scale + delta));
        },

        startPan(event) {
            this.panning = true;
            this.lastMouseX = event.clientX;
            this.lastMouseY = event.clientY;
        },

        onPan(event) {
            if (!this.panning) return;
            const dx = event.clientX - this.lastMouseX;
            const dy = event.clientY - this.lastMouseY;
            this.panX += dx;
            this.panY += dy;
            this.lastMouseX = event.clientX;
            this.lastMouseY = event.clientY;
        },

        endPan() {
            this.panning = false;
        },

        zoomIn() {
            this.scale = Math.min(2.2, this.scale + 0.12);
        },

        zoomOut() {
            this.scale = Math.max(0.55, this.scale - 0.12);
        },

        resetView() {
            this.scale = 1;
            this.panX = 0;
            this.panY = 0;
        },

        startAnimation() {
            if (this.animTimer) return;
            this.animTimer = setInterval(() => {
                this.animPhase += 0.04;
                if (this.animPhase > 1000) this.animPhase = 0;
            }, 33);
        },

        stopAnimation() {
            if (!this.animTimer) return;
            clearInterval(this.animTimer);
            this.animTimer = null;
        },

        getNodeMotion(nodeId, index) {
            const amp = this.mini ? 3 : 6;
            const seed = this.hashNode(nodeId) + index * 17;
            const speed = 0.8 + (seed % 7) * 0.11;
            const phase = this.animPhase * speed + (seed % 360);
            return {
                dx: Math.cos(phase) * amp,
                dy: Math.sin(phase * 0.8) * amp * 0.6,
            };
        },

        hashNode(value) {
            let h = 0;
            const s = String(value || '');
            for (let i = 0; i < s.length; i++) {
                h = ((h << 5) - h) + s.charCodeAt(i);
                h |= 0;
            }
            return Math.abs(h);
        },
    },
};
