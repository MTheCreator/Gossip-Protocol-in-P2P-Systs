/**
 * NetworkGraph.js — Native SVG topology renderer (no external graph library).
 */
const NetworkGraph = {
    name: 'NetworkGraph',
    props: {
        topology: { type: Object, default: () => ({ nodes: [], edges: [] }) },
        trafficEvents: { type: Array, default: () => [] },
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
                <span class="graph-status-chip">{{ graphEdges.length }} links</span>
                <span class="graph-status-chip">{{ flowEdgeCount }} active flows</span>
                <span class="graph-status-chip">{{ messageParticles.length }} msg points</span>
                <span class="graph-status-chip graph-status-chip-heartbeat">heartbeat</span>
                <span class="graph-status-chip graph-status-chip-message">broadcast</span>
                <span v-if="selectedChannel" class="graph-status-chip graph-status-chip-accent">#{{ selectedChannel }}</span>
            </div>

            <div class="graph-canvas-wrap">
                <div v-if="graphNodes.length === 0" class="graph-empty-state">
                    <div class="graph-empty-title">No topology data yet</div>
                    <div class="graph-empty-text">Start the monitor and let nodes report status to populate the network view.</div>
                </div>

                <svg
                    ref="graphSvg"
                    v-else
                    class="graph-fallback-svg"
                    viewBox="0 0 1200 800"
                    preserveAspectRatio="xMidYMid meet"
                    @contextmenu.prevent
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
                            :class="['graph-edge', edge.flowAlive ? 'graph-edge-active' : 'graph-edge-muted']"
                            stroke-linecap="round"
                        />

                        <text
                            v-for="edge in graphEdges"
                            :key="'prob-' + edge.id"
                            :x="edge.labelX"
                            :y="edge.labelY"
                            text-anchor="middle"
                            dominant-baseline="middle"
                            class="graph-edge-prob"
                            :fill="edge.probColor"
                        >
                            {{ edge.probText }}
                        </text>

                        <circle
                            v-for="hb in heartbeatParticles"
                            :key="hb.id"
                            :cx="hb.x"
                            :cy="hb.y"
                            :r="hb.r"
                            :opacity="hb.opacity"
                            class="graph-heartbeat-dot"
                        />

                        <circle
                            v-for="msg in messageParticles"
                            :key="msg.id"
                            :cx="msg.x"
                            :cy="msg.y"
                            :r="msg.r"
                            :opacity="msg.opacity"
                            class="graph-message-dot"
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
                                v-if="node.isSequencer"
                                :r="node.radius + 12"
                                fill="none"
                                stroke="#ef4444"
                                stroke-width="2"
                                opacity="0.4"
                                class="seq-pulse-ring"
                            />
                            <circle
                                v-if="node.isSeed && !node.isSequencer"
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
                            <text y="-22" text-anchor="middle" fill="#334155" font-size="12" font-weight="500" font-family="Inter, system-ui, sans-serif">{{ node.label }}</text>
                            <text y="32" text-anchor="middle" :fill="node.isSequencer ? '#ef4444' : node.status === 'core' ? '#d97706' : '#64748b'" font-size="10" font-weight="600" font-family="Inter, system-ui, sans-serif">{{ node.status }}</text>
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
            pushing: false,
            lastMouseX: 0,
            lastMouseY: 0,
            animPhase: 0,
            animTimer: null,
            frameNow: (typeof performance !== 'undefined' ? performance.now() : Date.now()),
            lastFrameNow: (typeof performance !== 'undefined' ? performance.now() : Date.now()),
            physicsNodes: {},
            pointerPush: {
                active: false,
                x: 600,
                y: 400,
                impulse: 1,
            },
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

            this.syncPhysicsNodes(nodes);

            return nodes.map((node, index) => {
                const state = this.ensureNodeState(node.id, index);
                const x = state.x;
                const y = state.y;
                const inChannel = this.channelNodeSet.size === 0 || this.channelNodeSet.has(node.id);

                let roleLabel = node.status || 'unknown';
                if (this.isSequencer(node)) roleLabel = 'sequencer';
                else if (this.isCore(node)) roleLabel = 'core';
                else if (node.is_seed) roleLabel = 'seed';

                return {
                    id: node.id,
                    raw: node,
                    x,
                    y,
                    label: node.address ? (':' + node.address.split(':').pop()) : (node.label || node.id.substring(0, 8)),
                    status: roleLabel,
                    isSeed: !!node.is_seed,
                    isSequencer: this.isSequencer(node),
                    radius: this.getRadius(node, inChannel),
                    fill: this.getFill(node, inChannel),
                    stroke: this.getStroke(node, inChannel),
                    opacity: inChannel ? 0.98 : 0.2,
                    animDelay: `${(index % 7) * 0.18}s`,
                };
            });
        },

        graphEdges() {
            const rawEdges = this.topology.edges || [];
            const lookup = new Map(this.graphNodes.map(node => [node.id, node]));
            const hasOverlay = this.channelNodeSet.size > 0;
            const directedSet = new Set(rawEdges.map(e => `${e.source}->${e.target}`));

            return rawEdges
                .map((edge, index) => {
                    const from = lookup.get(edge.source);
                    const to = lookup.get(edge.target);
                    if (!from || !to) return null;

                    const inOverlay = !hasOverlay || (this.channelNodeSet.has(edge.source) && this.channelNodeSet.has(edge.target));
                    const probability = typeof edge.probability === 'number' ? edge.probability : 1;

                    const reverseKey = `${edge.target}->${edge.source}`;
                    const hasReverse = directedSet.has(reverseKey);
                    const dx = to.x - from.x;
                    const dy = to.y - from.y;
                    const len = Math.hypot(dx, dy) || 1;
                    const offset = hasReverse && !this.mini
                        ? (edge.source < edge.target ? -4 : 4)
                        : 0;
                    const ox = (-dy / len) * offset;
                    const oy = (dx / len) * offset;

                    const flowAlive =
                        edge.status !== 'dead' &&
                        edge.source_status !== 'dead' &&
                        edge.target_status !== 'dead' &&
                        from.raw.status !== 'dead' &&
                        to.raw.status !== 'dead';

                    const labelX = ((from.x + ox) + (to.x + ox)) / 2;
                    const labelY = ((from.y + oy) + (to.y + oy)) / 2 - (this.mini ? 4 : 6);

                    return {
                        id: edge.id || `${edge.source}->${edge.target}-${index}`,
                        key: `${edge.source}->${edge.target}`,
                        source: edge.source,
                        target: edge.target,
                        directed: !!edge.directed,
                        x1: from.x + ox,
                        y1: from.y + oy,
                        x2: to.x + ox,
                        y2: to.y + oy,
                        stroke: edge.status === 'dead' || probability < 0.3 ? '#ef4444' : 'url(#edgeGrad)',
                        width: (this.mini ? 0.9 : 1.1) + probability * 2.0,
                        opacity: inOverlay ? 0.84 : 0.12,
                        dashArray: inOverlay ? '9 7' : '5 9',
                        dashOffset: -((this.animPhase * 42) + (index * 13)) % 200,
                        flowAlive,
                        labelX,
                        labelY,
                        probText: `${Math.round(probability * 100)}%`,
                        probColor: probability < 0.35 ? '#ef4444' : '#f97316',
                    };
                })
                .filter(Boolean);
        },

        flowEdgeCount() {
            return this.graphEdges.filter(e => e.flowAlive).length;
        },

        recentTraffic() {
            const nowMs = this.frameNow;
            const nowSec = nowMs / 1000;
            const events = Array.isArray(this.trafficEvents) ? this.trafficEvents : [];
            return events.filter(ev => {
                if (!ev || !ev.source_id || !ev.target_id) return false;
                const ts = typeof ev.ts === 'number' ? ev.ts : Number(ev.ts);
                if (!Number.isFinite(ts)) return true;
                return (nowSec - ts) <= 20;
            });
        },

        heartbeatParticles() {
            return this.buildParticles({
                messageTypes: new Set(['HEARTBEAT', 'HEARTBEAT_ACK', 'SEQUENCER_HEARTBEAT']),
                durationMs: this.mini ? 1800 : 2300,
                radius: this.mini ? 2.0 : 2.9,
                baseOpacity: this.mini ? 0.76 : 0.9,
            });
        },

        messageParticles() {
            return this.buildParticles({
                messageTypes: new Set(['GOSSIP', 'SEQUENCE_ASSIGN']),
                durationMs: this.mini ? 2000 : 2500,
                radius: this.mini ? 2.8 : 4.2,
                baseOpacity: this.mini ? 0.92 : 1.0,
            });
        },

        viewTransform() {
            return `translate(${this.panX} ${this.panY}) scale(${this.scale})`;
        },
    },

    methods: {
        isSequencer(node) {
            const ss = node.sequencer_status || {};
            for (const ch in ss) {
                if (ss[ch].is_sequencer) return true;
            }
            return false;
        },

        isCore(node) {
            const cs = node.core_status || {};
            for (const ch in cs) {
                if (cs[ch].is_core) return true;
            }
            return false;
        },

        getRadius(node, inChannel) {
            if (!inChannel) return this.mini ? 8 : 10;
            if (this.isSequencer(node)) return this.mini ? 18 : 24;
            if (this.isCore(node)) return this.mini ? 14 : 18;
            if (node.is_seed) return this.mini ? 16 : 20;
            if (node.status === 'suspect') return this.mini ? 13 : 16;
            if (node.status === 'dead') return this.mini ? 9 : 11;
            return this.mini ? 11 : 14;
        },

        getFill(node, inChannel) {
            if (!inChannel) return '#cbd5e1';
            if (node.status === 'dead') return '#cbd5e1';
            if (this.isSequencer(node)) return '#ef4444';
            if (this.isCore(node)) return '#f59e0b';
            if (node.is_seed) return '#6366f1';
            if (node.status === 'suspect') return '#facc15';
            return '#10b981';
        },

        getStroke(node, inChannel) {
            if (!inChannel) return '#94a3b8';
            if (node.status === 'dead') return '#94a3b8';
            if (this.isSequencer(node)) return '#dc2626';
            if (this.isCore(node)) return '#d97706';
            if (node.is_seed) return '#4338ca';
            if (node.status === 'suspect') return '#ca8a04';
            return '#059669';
        },

        buildParticles({ messageTypes, durationMs, radius, baseOpacity }) {
            const particles = [];
            const now = this.frameNow;
            const edgeMap = new Map();
            for (const edge of this.graphEdges) {
                edgeMap.set(edge.key, edge);
            }

            for (const ev of this.recentTraffic) {
                const msgType = String(ev.msg_type || '').toUpperCase();
                if (!messageTypes.has(msgType)) continue;

                const edgeKey = `${ev.source_id}->${ev.target_id}`;
                const edge = edgeMap.get(edgeKey);
                if (!edge || !edge.flowAlive) continue;

                const startMs = this.eventStartMs(ev, now);
                const progress = (now - startMs) / durationMs;
                if (progress < 0 || progress > 1) continue;

                const tailFade = progress < 0.86
                    ? 1
                    : Math.max(0.12, 1 - ((progress - 0.86) / 0.14));

                particles.push({
                    id: `${msgType}-${ev.id || `${edgeKey}-${startMs}`}`,
                    x: edge.x1 + (edge.x2 - edge.x1) * progress,
                    y: edge.y1 + (edge.y2 - edge.y1) * progress,
                    r: radius,
                    opacity: baseOpacity * tailFade,
                });
            }

            return particles;
        },

        eventStartMs(ev, fallbackNowMs) {
            const arrival = Number(ev.arrival_ms);
            if (Number.isFinite(arrival) && arrival > 0) return arrival;
            const ts = Number(ev.ts);
            if (Number.isFinite(ts) && ts > 0) return ts * 1000;
            return fallbackNowMs;
        },

        onWheel(event) {
            const delta = event.deltaY > 0 ? -0.08 : 0.08;
            this.scale = Math.min(2.2, Math.max(0.55, this.scale + delta));
        },

        startPan(event) {
            const world = this.toWorldPoint(event);
            this.pointerPush.active = true;
            this.pointerPush.x = world.x;
            this.pointerPush.y = world.y;
            this.pointerPush.impulse = 1.0;

            this.pushing = event.button === 0 && !event.shiftKey;
            this.panning = !this.pushing;
            this.lastMouseX = event.clientX;
            this.lastMouseY = event.clientY;
        },

        onPan(event) {
            const dx = event.clientX - this.lastMouseX;
            const dy = event.clientY - this.lastMouseY;

            if (this.panning) {
                this.panX += dx;
                this.panY += dy;
            }

            if (this.pushing) {
                const world = this.toWorldPoint(event);
                this.pointerPush.active = true;
                this.pointerPush.x = world.x;
                this.pointerPush.y = world.y;
                this.pointerPush.impulse = Math.min(1.8, 1 + Math.hypot(dx, dy) / 32);
            }

            this.lastMouseX = event.clientX;
            this.lastMouseY = event.clientY;
        },

        endPan() {
            this.panning = false;
            this.pushing = false;
            this.pointerPush.active = false;
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
                const now = (typeof performance !== 'undefined' ? performance.now() : Date.now());
                const dt = Math.max(0.008, Math.min(0.05, (now - this.lastFrameNow) / 1000));
                this.lastFrameNow = now;
                this.stepPhysics(dt);
                this.frameNow = now;
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

        syncPhysicsNodes(nodes) {
            const keep = new Set();
            nodes.forEach((node, index) => {
                keep.add(node.id);
                this.ensureNodeState(node.id, index);
            });
            for (const id in this.physicsNodes) {
                if (!keep.has(id)) delete this.physicsNodes[id];
            }
        },

        ensureNodeState(nodeId, index = 0) {
            if (this.physicsNodes[nodeId]) return this.physicsNodes[nodeId];
            const seed = this.hashNode(nodeId) + index * 97;
            const minX = 90;
            const maxX = 1110;
            const minY = 80;
            const maxY = 720;
            const spanX = maxX - minX;
            const spanY = maxY - minY;
            const randX = minX + (seed % 1000) / 1000 * spanX;
            const randY = minY + ((seed * 31) % 1000) / 1000 * spanY;
            const state = {
                x: randX,
                y: randY,
                vx: 0,
                vy: 0,
                anchorX: randX,
                anchorY: randY,
            };
            this.physicsNodes[nodeId] = state;
            return state;
        },

        stepPhysics(dt) {
            const nodes = this.topology.nodes || [];
            if (nodes.length === 0) return;

            this.syncPhysicsNodes(nodes);

            const states = nodes.map((node, idx) => ({
                id: node.id,
                s: this.ensureNodeState(node.id, idx),
                fx: 0,
                fy: 0,
            }));
            const byId = new Map(states.map(v => [v.id, v]));
            const dtN = Math.max(0.45, Math.min(1.35, dt * 42));

            // Repulsion to keep nodes separated and create a mesh look.
            const repel = this.mini ? 5200 : 9000;
            for (let i = 0; i < states.length; i++) {
                for (let j = i + 1; j < states.length; j++) {
                    const a = states[i];
                    const b = states[j];
                    let dx = b.s.x - a.s.x;
                    let dy = b.s.y - a.s.y;
                    const d2 = Math.max(90, dx * dx + dy * dy);
                    const d = Math.sqrt(d2);
                    if (d < 0.001) {
                        dx = 1;
                        dy = 0;
                    }
                    const f = repel / d2;
                    const fx = (dx / d) * f;
                    const fy = (dy / d) * f;
                    a.fx -= fx;
                    a.fy -= fy;
                    b.fx += fx;
                    b.fy += fy;
                }
            }

            // Spring links on current topology edges.
            const spring = this.mini ? 0.006 : 0.009;
            const restLen = this.mini ? 150 : 210;
            for (const edge of (this.topology.edges || [])) {
                const a = byId.get(edge.source);
                const b = byId.get(edge.target);
                if (!a || !b) continue;
                const dx = b.s.x - a.s.x;
                const dy = b.s.y - a.s.y;
                const d = Math.max(1, Math.hypot(dx, dy));
                const stretch = d - restLen;
                const f = stretch * spring;
                const fx = (dx / d) * f;
                const fy = (dy / d) * f;
                a.fx += fx;
                a.fy += fy;
                b.fx -= fx;
                b.fy -= fy;
            }

            // Pull nodes gently toward deterministic random anchors so the
            // long-term layout remains mesh-like and irregular (not circular).
            for (const n of states) {
                n.fx += (n.s.anchorX - n.s.x) * 0.0032;
                n.fy += (n.s.anchorY - n.s.y) * 0.0032;
            }

            // Direct interaction: pointer pushes nearby nodes away.
            if (this.pointerPush.active) {
                const r = this.mini ? 140 : 190;
                const impulse = (this.pointerPush.impulse || 1) * (this.mini ? 0.8 : 1.0);
                for (const n of states) {
                    const dx = n.s.x - this.pointerPush.x;
                    const dy = n.s.y - this.pointerPush.y;
                    const d = Math.max(1, Math.hypot(dx, dy));
                    if (d > r) continue;
                    const t = 1 - (d / r);
                    const push = t * t * impulse * 2.3;
                    n.s.vx += (dx / d) * push;
                    n.s.vy += (dy / d) * push;
                }
                this.pointerPush.impulse = Math.max(1, this.pointerPush.impulse * 0.92);
            }

            // Integrate and confine.
            const damping = this.mini ? 0.9 : 0.93;
            for (const n of states) {
                n.s.vx = (n.s.vx + n.fx * dtN) * damping;
                n.s.vy = (n.s.vy + n.fy * dtN) * damping;
                n.s.x += n.s.vx * dtN;
                n.s.y += n.s.vy * dtN;

                const minX = 50;
                const maxX = 1150;
                const minY = 50;
                const maxY = 750;

                if (n.s.x < minX) { n.s.x = minX; n.s.vx *= -0.35; }
                if (n.s.x > maxX) { n.s.x = maxX; n.s.vx *= -0.35; }
                if (n.s.y < minY) { n.s.y = minY; n.s.vy *= -0.35; }
                if (n.s.y > maxY) { n.s.y = maxY; n.s.vy *= -0.35; }
            }
        },

        toWorldPoint(event) {
            const svg = this.$refs.graphSvg;
            if (!svg) return { x: 600, y: 400 };
            const rect = svg.getBoundingClientRect();
            const viewW = 1200;
            const viewH = 800;
            const sx = (event.clientX - rect.left) * (viewW / Math.max(1, rect.width));
            const sy = (event.clientY - rect.top) * (viewH / Math.max(1, rect.height));
            return {
                x: (sx - this.panX) / this.scale,
                y: (sy - this.panY) / this.scale,
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
