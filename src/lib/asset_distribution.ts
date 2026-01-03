import * as d3 from "d3";
import _ from "lodash";
import { formatCurrency, type AssetDistribution, type Legend } from "./utils";
import { generateColorScheme } from "./colors";

export function pieData(distributions: AssetDistribution[]) {
    return d3
        .pie<AssetDistribution>()
        .value((d) => d.amount)
        .sort((a, b) => a.category.localeCompare(b.category))(distributions);
}

export function renderAssetDistribution(
    id: string,
    distributions: AssetDistribution[]
): {
    legends: Legend[];
    renderer: (distributions: AssetDistribution[]) => void;
} {
    const categories = distributions.map((d) => d.category);
    const color = generateColorScheme(categories);

    const tooltipContent = (d: d3.PieArcDatum<AssetDistribution>) => {
        return `${d.data.category}: ${formatCurrency(d.data.amount)} (${d.data.percentage.toFixed(1)}%)`;
    };

    const renderer = (data: AssetDistribution[]) => {
        if (_.isEmpty(data)) {
            return;
        }

        const svgElement = document.getElementById(id.substring(1));
        if (!svgElement) {
            console.warn(`Element ${id} not found in DOM`);
            return;
        }

        const svg = d3.select(id);
        if (svg.empty()) {
            console.warn(`SVG element ${id} not found`);
            return;
        }

        // Clear existing content
        svg.selectAll("*").remove();

        // Calculate available width using parent element (same pattern as other charts)
        const container = svgElement.parentElement;
        const width = container ? Math.max(container.clientWidth, 250) : 250;
        const height = 250;
        const radius = Math.min(width, height) / 2 - 20;

        svg.attr("width", width).attr("height", height);

        const g = svg
            .append("g")
            .attr("transform", `translate(${width / 2}, ${height / 2})`);

        const arc = d3
            .arc<d3.PieArcDatum<AssetDistribution>>()
            .innerRadius(0)
            .outerRadius(radius);

        const pieDataResult = pieData(data);
        const colorScale = generateColorScheme(data.map((d) => d.category));

        const paths = g
            .selectAll("path")
            .data(pieDataResult)
            .enter()
            .append("path")
            .attr("fill", (d) => colorScale(d.data.category))
            .attr("d", arc)
            .attr("data-tippy-content", tooltipContent)
            .style("cursor", "pointer");

        // Add labels with better visibility
        const labels = g
            .selectAll("text")
            .data(pieDataResult)
            .enter()
            .append("text")
            .attr("transform", (d) => {
                const [x, y] = arc.centroid(d);
                return `translate(${x}, ${y})`;
            })
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "middle")
            .attr("font-size", "16px")
            .attr("font-weight", "bold")
            .attr("fill", "#ffffff")
            .style("text-shadow", "1px 1px 2px rgba(0,0,0,0.8), -1px -1px 2px rgba(0,0,0,0.8), 1px -1px 2px rgba(0,0,0,0.8), -1px 1px 2px rgba(0,0,0,0.8)")
            .style("pointer-events", "none")
            .text((d) => {
                // Only show label if slice is large enough
                if (d.endAngle - d.startAngle > 0.15) {
                    return `${d.data.percentage.toFixed(1)}%`;
                }
                return "";
            });
    };

    // Generate legends
    const legends: Legend[] = distributions.map((d) => ({
        label: `${d.category} (${d.percentage.toFixed(1)}%)`,
        color: color(d.category),
        shape: "square" as const
    }));

    // Try initial render, but don't fail if element doesn't exist yet
    // The caller can call renderer again when DOM is ready
    const svgElement = document.getElementById(id.substring(1));
    if (svgElement) {
        renderer(distributions);
    }

    return { legends, renderer };
}

