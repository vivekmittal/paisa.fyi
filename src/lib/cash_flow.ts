import {
  type Graph,
  type Node,
  formatCurrencyCrude,
  restName,
  type CashFlow,
  skipTicks,
  lastName,
  parentName,
  rem,
  type Legend
} from "$lib/utils";
import * as d3 from "d3";
import { sankeyCircular, sankeyJustify } from "d3-sankey-circular";
import { pathArrows } from "d3-path-arrows";
import _ from "lodash";
import { firstName } from "$lib/utils";
import { tooltip } from "$lib/utils";
import { formatCurrency } from "$lib/utils";
import { iconify } from "$lib/icon";
import { willClearTippy } from "../store";
import COLORS, { generateColorScheme } from "./colors";
import textures from "textures";
import chroma from "chroma-js";

// "Where did my income go?" One stacked bar per month shows what you did with
// your money (spent, taxed, repaid loans, invested). A green tick marks income,
// so the gap between the top of the bar and the tick is what you kept.
export function renderMonthlyFlow(
  id: string,
  options = {
    rotate: true,
    balance: 0
  }
) {
  const MAX_BAR_WIDTH = rem(22);
  const svg = d3.select(id),
    margin = {
      top: rem(10),
      right: rem(10),
      bottom: options.rotate ? rem(50) : rem(22),
      left: rem(42)
    },
    width =
      document.getElementById(id.substring(1)).parentElement.clientWidth -
      margin.left -
      margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  const outflowKeys = ["expenses", "tax", "liabilities", "investment"];
  const colors = [COLORS.expenses, COLORS.neutralBar, COLORS.liabilities, COLORS.assets];

  const x = d3.scaleBand().range([0, width]).paddingInner(0.25).paddingOuter(0.1),
    y = d3.scaleLinear().range([height, 0]),
    z = d3.scaleOrdinal<string>(colors).domain(outflowKeys);

  const xAxis = g
    .append("g")
    .attr("class", "axis x")
    .attr("transform", "translate(0," + height + ")");

  const yAxis = g.append("g").attr("class", "axis y");
  const groups = g.append("g");
  const incomeTicks = g.append("g");
  const tooltipRects = g.append("g");

  let firstRender = true;

  const outflow = (c: CashFlow) => ({
    expenses: Math.max(c.expenses, 0),
    tax: Math.max(c.tax, 0),
    liabilities: c.liabilities < 0 ? -c.liabilities : 0,
    investment: Math.max(c.investment, 0)
  });

  const renderer = function (cashFlows: CashFlow[]) {
    const key = (c: CashFlow) => c.date.format("MMM YYYY");
    const tops = _.flatMap(cashFlows, (c) => [c.income, _.sum(_.values(outflow(c)))]);

    x.domain(_.map(cashFlows, key));
    y.domain([0, d3.max(tops) || 1]).nice();

    const t = svg.transition().duration(firstRender ? 0 : 600);
    firstRender = false;

    const axis = xAxis
      .transition(t)
      .call(
        d3
          .axisBottom(x)
          .tickSize(0)
          .tickFormat(skipTicks(rem(52), x, (d) => d.toString()))
      )
      .selectAll("text")
      .attr("y", 10)
      .attr("x", -8)
      .attr("dy", ".35em");

    if (options.rotate) {
      axis.attr("transform", "rotate(-45)").style("text-anchor", "end");
    }

    yAxis
      .transition(t)
      .call(d3.axisLeft(y).ticks(5).tickSize(-width).tickFormat(formatCurrencyCrude));

    const barWidth = Math.min(x.bandwidth(), MAX_BAR_WIDTH);
    const barX = (c: CashFlow) => x(key(c)) + (x.bandwidth() - barWidth) / 2;

    const gbars = groups
      .selectAll<SVGGElement, CashFlow>("g.group")
      .data(cashFlows, (d: any) => key(d))
      .join("g")
      .attr("class", "group");

    gbars
      .selectAll("rect")
      .data((c) => {
        const stack = d3.stack().keys(outflowKeys)([outflow(c)] as any);
        return stack.map((layer) => ({ layer, c }));
      })
      .join("rect")
      .attr("fill", (d) => z(d.layer.key))
      .attr("fill-opacity", 0.9)
      .attr("x", (d) => barX(d.c))
      .attr("width", barWidth)
      .transition(t)
      .attr("y", (d) => y(d.layer[0][1]))
      .attr("height", (d) => Math.max(0, y(d.layer[0][0]) - y(d.layer[0][1])));

    incomeTicks
      .selectAll("line")
      .data(cashFlows)
      .join("line")
      .attr("stroke", COLORS.income)
      .attr("stroke-width", 3)
      .attr("stroke-linecap", "round")
      .attr("x1", (c) => barX(c) - 3)
      .attr("x2", (c) => barX(c) + barWidth + 3)
      .attr("y1", (c) => y(c.income))
      .attr("y2", (c) => y(c.income));

    tooltipRects
      .selectAll("rect")
      .data(cashFlows)
      .join("rect")
      .attr("fill", "transparent")
      .attr("data-tippy-content", (c: CashFlow) => {
        const o = outflow(c);
        const kept = c.income - _.sum(_.values(o));
        const right = "has-text-weight-bold has-text-right";
        return tooltip(
          [
            ["Income", [formatCurrency(c.income), right]],
            ["Expenses", [formatCurrency(o.expenses), right]],
            ["Tax", [formatCurrency(o.tax), right]],
            ["Loan repayment", [formatCurrency(o.liabilities), right]],
            ["Invested", [formatCurrency(o.investment), right]],
            ["Left over", [formatCurrency(kept), right]]
          ],
          { header: c.date.format("MMM YYYY") }
        );
      })
      .attr("x", (c) => x(key(c)) - (x.step() - x.bandwidth()) / 2)
      .attr("y", 0)
      .attr("height", height)
      .attr("width", x.step());
  };

  const legends: Legend[] = [
    { label: "Income", color: COLORS.income, shape: "line" },
    { label: "Expenses", color: z("expenses"), shape: "square" },
    { label: "Tax", color: z("tax"), shape: "square" },
    { label: "Loan repayment", color: z("liabilities"), shape: "square" },
    { label: "Invested", color: z("investment"), shape: "square" }
  ];

  return { renderer, legends };
}

export function renderFlow(graph: Graph) {
  const id = "#d3-expense-flow";
  const svg = d3.select(id);
  const margin = { top: rem(60), right: rem(20), bottom: rem(40), left: rem(20) },
    width =
      Math.max(document.getElementById(id.substring(1)).parentElement.clientWidth, 1000) -
      margin.left -
      margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom;

  willClearTippy.update((n) => n + 1);
  svg.selectAll("*").remove();

  if (graph == null) {
    return;
  }

  const accounts = _.chain(graph.nodes)
    .map((n) => firstName(n.name))
    .uniq()
    .sort()
    .value();
  const color = generateColorScheme(accounts);

  const g = svg
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  const sankey = sankeyCircular()
    .nodeWidth(rem(20))
    .nodePaddingRatio(0.7)
    .size([width, height])
    .nodeId((d: Node) => d.id)
    .nodeAlign(sankeyJustify)
    .iterations(64)
    .circularLinkGap(2);

  const linkG = g
    .append("g")
    .attr("class", "links")
    .attr("fill", "none")
    .attr("stroke-opacity", 0.2)
    .selectAll("path");

  const nodeG = g
    .append("g")
    .attr("class", "nodes")
    .attr("font-family", "sans-serif")
    .attr("font-size", "0.754rem")
    .selectAll("g");

  const sankeyData = sankey(graph);
  const sankeyNodes = sankeyData.nodes;
  const sankeyLinks = sankeyData.links;

  d3.extent(sankeyNodes, (d: any) => d.depth);

  const node = nodeG.data(sankeyNodes).enter().append("g");

  node
    .append("rect")
    .attr("x", (d: any) => d.x0)
    .attr("y", (d: any) => d.y0)
    .attr("height", (d: any) => d.y1 - d.y0)
    .attr("width", (d: any) => d.x1 - d.x0)
    .style("fill", (d: any) => color(firstName(d.name)))
    .attr("data-tippy-content", (d: any) =>
      tooltip([
        ["Account", iconify(d.name)],
        ["Total", [formatCurrency(d.value), "has-text-right has-text-weight-bold"]]
      ])
    );

  node
    .append("text")
    .attr("x", (d: any) => (d.x0 + d.x1) / 2)
    .attr("y", (d: any) => d.y0 - 12)
    .attr("dx", (d: any) => {
      if (_.isEmpty(d.sourceLinks)) {
        return "10px";
      }

      if (_.isEmpty(d.targetLinks)) {
        return "-10px";
      }

      return "0px";
    })
    .attr("dy", "0.35em")
    .attr("text-anchor", (d: any) => {
      if (_.isEmpty(d.sourceLinks)) {
        return "end";
      }

      if (_.isEmpty(d.targetLinks)) {
        return "start";
      }

      return "middle";
    })
    .classed("svg-text-grey-dark", true)
    .text((d: any) => `${name(d)} ${formatCurrencyCrude(d.value)}`);

  const link = linkG.data(sankeyLinks).enter().append("g");

  link
    .append("path")
    .attr("class", "sankey-link")
    .attr("d", (link: any) => link.path)
    .style("stroke-width", (d: any) => Math.max(1, d.width))
    .style("opacity", 0.5)
    .style("stroke", (d: any) => color(firstName(d.target.name)))
    .attr("data-tippy-content", (d: any) =>
      tooltip([
        ["Debit Account", iconify(d.source.name)],
        ["Credit Account", iconify(d.target.name)],
        ["Total", [formatCurrency(d.value), "has-text-right has-text-weight-bold"]]
      ])
    );

  const arrows = pathArrows()
    .arrowLength(10)
    .gapLength(150)
    .arrowHeadSize(3)
    .path((link: any) => link.path);

  linkG.data(sankeyLinks).enter().append("g").attr("class", "g-arrow").call(arrows);

  const legends: Legend[] = _.map(accounts, (k) => ({
    label: k,
    color: color(k),
    shape: "square"
  }));
  return legends;
}

function name(node: Node) {
  let name: string, group: string;
  if (node.name.startsWith("Income") || node.name.startsWith("Expenses")) {
    name = lastName(node.name);
    group = parentName(node.name);
  } else {
    name = restName(node.name);
    group = firstName(node.name);
  }

  return iconify(name, { group: group });
}
