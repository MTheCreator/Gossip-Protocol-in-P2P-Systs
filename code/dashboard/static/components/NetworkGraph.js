/**
 * NetworkGraph.js — D3 force-directed graph as a Vue component
 *
 * Props:
 *   topology        — { nodes: [...], edges: [...] }
 *   selectedChannel — channel name for overlay (or "")
 *   channelDetail   — detail object from /api/channels/{name}/detail
 *
 * Emits:
 *   node-click(node) — when a node circle is clicked
 */
const NetworkGraph = {
    name: 'NetworkGraph',
    props: {
        topology: { type: Object, default: () => ({ nodes: [], edges: [] }) },
        selectedChannel: { type: String, default: '' },
        channelDetail: { type: Object, default: null },
    },
    emits: ['node-click'],
    template: `<svg ref="svg"></svg>`,

    data() {
        return {
            simulation: null,
            svgSel: null,
            linkGroup: null,
            nodeGroup: null,
            labelGroup: null,
            width: 800,
            height: 600,
        };
    },

    mounted() {
        this.initGraph();
        this._resizeObs = new ResizeObserver(() => this.resize());
        this._resizeObs.observe(this.$refs.svg.parentElement);
    },

    beforeUnmount() {
        if (this._resizeObs) this._resizeObs.disconnect();
        if (this.simulation) this.simulation.stop();
    },

    watch: {
        topology: { handler: 'updateGraph', deep: true },
        selectedChannel() { this.applyOverlay(); },
        channelDetail: { handler: 'applyOverlay', deep: true },
    },

    methods: {
        resize() {
            const parent = this.$refs.svg.parentElement;
            this.width = parent.clientWidth;
            this.height = parent.clientHeight;
            d3.select(this.$refs.svg).attr('width', this.width).attr('height', this.height);
            if (this.simulation) {
                this.simulation.force('center', d3.forceCenter(this.width / 2, this.height / 2));
                this.simulation.alpha(0.1).restart();
            }
        },

        initGraph() {
            const svg = d3.select(this.$refs.svg);
            const parent = this.$refs.svg.parentElement;
            this.width = parent.clientWidth;
            this.height = parent.clientHeight;
            svg.attr('width', this.width).attr('height', this.height);

            // Zoom
            const g = svg.append('g');
            svg.call(d3.zoom().scaleExtent([0.2, 4]).on('zoom', (e) => {
                g.attr('transform', e.transform);
            }));

            this.linkGroup = g.append('g').attr('class', 'links');
            this.nodeGroup = g.append('g').attr('class', 'nodes');
            this.labelGroup = g.append('g').attr('class', 'labels');
            this.svgSel = svg;

            this.simulation = d3.forceSimulation()
                .force('link', d3.forceLink().id(d => d.id).distance(120))
                .force('charge', d3.forceManyBody().strength(-300))
                .force('center', d3.forceCenter(this.width / 2, this.height / 2))
                .force('collision', d3.forceCollide(30));

            this.updateGraph();
        },

        updateGraph() {
            if (!this.simulation) return;
            const nodes = this.topology.nodes.map(n => ({ ...n }));
            const edges = this.topology.edges.map(e => ({ ...e }));

            // --- Links ---
            const links = this.linkGroup.selectAll('line').data(edges, d => d.source + '-' + d.target);
            links.exit().remove();
            const linksEnter = links.enter().append('line');
            const allLinks = linksEnter.merge(links)
                .attr('stroke', d => {
                    const p = d.probability || 1;
                    return p > 0.7 ? '#22c55e' : p > 0.4 ? '#eab308' : '#ef4444';
                })
                .attr('stroke-opacity', d => 0.3 + (d.probability || 1) * 0.5)
                .attr('stroke-width', d => 1 + (d.probability || 1) * 2);

            // --- Nodes ---
            const nodesSel = this.nodeGroup.selectAll('circle').data(nodes, d => d.id);
            nodesSel.exit().remove();
            const nodesEnter = nodesSel.enter().append('circle')
                .attr('r', 14)
                .attr('cursor', 'pointer')
                .on('click', (event, d) => this.$emit('node-click', d))
                .call(this.drag(this.simulation));
            const allNodes = nodesEnter.merge(nodesSel)
                .attr('fill', d => this.nodeColor(d))
                .attr('stroke', d => d.is_seed ? '#3b82f6' : '#475569')
                .attr('stroke-width', d => d.is_seed ? 3 : 1.5);

            // --- Labels ---
            const labelsSel = this.labelGroup.selectAll('text').data(nodes, d => d.id);
            labelsSel.exit().remove();
            const labelsEnter = labelsSel.enter().append('text')
                .attr('text-anchor', 'middle')
                .attr('dy', -20)
                .attr('font-size', '10px')
                .attr('fill', '#94a3b8');
            const allLabels = labelsEnter.merge(labelsSel).text(d => d.label);

            // Simulation
            this.simulation.nodes(nodes).on('tick', () => {
                allLinks
                    .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
                    .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
                allNodes.attr('cx', d => d.x).attr('cy', d => d.y);
                allLabels.attr('x', d => d.x).attr('y', d => d.y);
            });

            this.simulation.force('link').links(edges);
            this.simulation.alpha(0.3).restart();

            // Store refs for overlay
            this._allNodes = allNodes;
            this._allLabels = allLabels;
            this._allLinks = allLinks;
            this._nodeData = nodes;

            this.applyOverlay();
        },

        nodeColor(d) {
            if (d.status === 'dead') return '#64748b';
            if (d.is_seed) return '#3b82f6';
            if (d.status === 'suspect') return '#eab308';
            return '#22c55e';
        },

        applyOverlay() {
            if (!this._allNodes) return;
            const ch = this.selectedChannel;
            const detail = this.channelDetail;

            if (!ch || !detail) {
                // Reset: full opacity, default colors
                this._allNodes
                    .attr('fill', d => this.nodeColor(d))
                    .attr('opacity', 1)
                    .attr('r', 14);
                this._allLabels.attr('opacity', 1);
                this._allLinks.attr('opacity', 0.6);
                return;
            }

            // Build lookup: node_id -> subscriber info
            const subMap = {};
            (detail.subscribers || []).forEach(s => { subMap[s.node_id] = s; });

            this._allNodes
                .attr('opacity', d => subMap[d.id] ? 1 : 0.15)
                .attr('fill', d => {
                    const s = subMap[d.id];
                    if (!s) return '#64748b';
                    if (s.is_sequencer) return '#ef4444';
                    if (s.is_core) return '#f59e0b';
                    return '#22c55e';
                })
                .attr('r', d => {
                    const s = subMap[d.id];
                    if (!s) return 10;
                    if (s.is_sequencer) return 20;
                    if (s.is_core) return 17;
                    return 14;
                });

            this._allLabels.attr('opacity', d => subMap[d.id] ? 1 : 0.15);
            this._allLinks.attr('opacity', 0.2);
        },

        drag(simulation) {
            return d3.drag()
                .on('start', (event, d) => {
                    if (!event.active) simulation.alphaTarget(0.3).restart();
                    d.fx = d.x; d.fy = d.y;
                })
                .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
                .on('end', (event, d) => {
                    if (!event.active) simulation.alphaTarget(0);
                    d.fx = null; d.fy = null;
                });
        },
    },
};
