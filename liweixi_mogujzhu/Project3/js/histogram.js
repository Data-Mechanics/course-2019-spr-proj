var dataset = [50, 43, 120, 87, 99, 167, 142];  // 数据集

var width = 400;    // svg可视区域宽度
var height = 400;   // svg可视区域高度
var svg = d3.select("body")
        .append("svg")
        .attr("width", width).attr("height", height);

var padding = {top: 20, right: 20, bottom: 20, left: 50};   // 边距

var xAxisWidth = 300; // x轴宽度
var yAxisWidth = 300; // y轴宽度

/* x轴比例尺(序数比例尺) */
var xScale = d3.scale.ordinal()
        .domain(d3.range(dataset.length))
        .rangeRoundBands([0, xAxisWidth], 0.2);

/* y轴比例尺(线性比例尺) */
var yScale = d3.scale.linear()
        .domain([0, d3.max(dataset)])
        .range([0, yAxisWidth]);

/* rect */
var rect = svg.selectAll("rect")
        .data(dataset)
        .enter()
        .append("rect")
        .call(rectFun);

/* text */
var text = svg.selectAll("text")
        .data(dataset)
        .enter()
        .append("text")
        .call(textFun);

/* 添加坐标轴 */
var xAxis = d3.svg.axis().scale(xScale).orient("bottom");
yScale.range([yAxisWidth, 0]);  // 重新设置y轴比例尺的值域,与原来的相反
var yAxis = d3.svg.axis().scale(yScale).orient("left");

svg.append("g").attr("class", "axis")
        .attr("transform", "translate("+ padding.left +","+ (height - padding.bottom) +")")
        .call(xAxis);

svg.append("g").attr("class", "axis")
        .attr("transform", "translate("+ padding.left +","+ (height - padding.bottom - yAxisWidth) +")")
        .call(yAxis);

/* rect处理函数 */
function rectFun(selection) {
    selection.attr("fill", "steelblue")
            .attr("x", function(d, i){
                return padding.left + xScale(i);
            })
            .attr("y", function(d){
                return height - padding.bottom - yScale(d);
            })
            .attr("width", xScale.rangeBand())
            .attr("height", function(d){
                return yScale(d);
            });
}

/* text处理函数 */
function textFun(selection){
    selection.attr("fill", "white")
            .attr("font-size", "14px").attr("text-anchor", "middle")
            .attr("x", function(d, i){
                return padding.left + xScale(i);
            })
            .attr("y", function(d){
                return height - padding.bottom - yScale(d);
            })
            .attr("dx", xScale.rangeBand()/2).attr("dy", "1em")
            .text(function(d){
                return d;
            });
}
