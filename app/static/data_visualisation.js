console.log("data_visualisation");

import React from "react";
import axios from "axios";
import * as d3 from "d3";
import { render } from "react-dom";

window.React = React;

class App extends React.Component {

    componentDidMount() {
        axios.get("/data-visualisation-data")
            .then(response => this.setData(response.data))
            .catch(response => alert(`failed to get data. ${response}`));
    }

    constructor(props){
        super(props);
        this.state = {variable: "country"};

        this.play = this.play.bind(this);
        this.setData = this.setData.bind(this);
        this.setDate = this.setDate.bind(this);
        this.setNextDate = this.setNextDate.bind(this);
    }

    play(e) {
        console.log("App.play");
        this.interval = window.setInterval(this.setNextDate, 1000);
    }

    setData(data) {
        let processedData = {...data};
        processedData = Object.keys(processedData).reduce(
            (acc, key)=>{
                let data = processedData[key];
                data = data.map(d=>({...d, quarter: new Date(d.quarter)}));
                data = data.sort((a, b)=>a.quarter - b.quarter);
                acc[key] = data;
                return acc;
            },
            {}
        );

        let datesData = processedData["interest_by_countries_and_quarter"];
        let dates = datesData.reduce(
            (acc, d)=> {
                acc[[d.quarter]] = d.quarter;
                return acc;
            },
            {}
        );
        dates = Object.values(dates);
        dates = dates.sort((a, b)=>a - b);

        let countriesData = processedData["top_countries"];
        let countries = datesData.reduce(
            (acc, d) => {
                acc[[d.country]] = d.country;
                return acc;
            },
            {}
        );
        countries = Object.values(countries);
        let countryColourScale = d3.scaleOrdinal()
            .domain(countries)
            .range(d3.schemeTableau10);

        let sectorsData = processedData["top_sectors"];
        let sectors = datesData.reduce(
            (acc, d) => {
                acc[[d.country]] = d.country;
                return acc;
            },
            {}
        );
        let schema = d3.schemeTableau10; // d3.schemeSet3
        sectors = Object.values(sectors);
        let sectorColourScale = d3.scaleOrdinal()
            .domain(sectors)
            .range(schema);
        
        this.setState(
            {
                countries: countries,
                countryColourScale: countryColourScale,
                data: processedData,
                dates: dates,
                sectorColourScale: sectorColourScale,
            }
        );
    }

    setDate(date) {
        console.log("this.interval");
        console.log(this.interval);
        console.log(date);
        if(this.interval !== undefined && date === this.state.dates[this.state.dates.length - 1]) {
            console.log("clearInterval");
            window.clearInterval(this.interval);
        }
        this.setState({date: date});
    }

    setNextDate() {
        console.log("App.setNextDate");
        console.log(this.state.dates);
        let date = this.state.date;
        console.log(date);
        if(
            date !== undefined &&
                this.state.dates.length > 0 &&
                date != this.state.dates[this.state.dates.length - 1]) {
            let index = this.state.dates.indexOf(this.state.date);
            let nextDate = this.state.dates[index+1];
            this.setDate(nextDate);
        }
    }
    
    render() {
        console.log("App.render");
        console.log(this.state);

        let slider = "";
        let dates = this.state.dates;
        if(dates !== undefined  && dates.length > 0) {
            slider = <input
                       className="custom-range"
                       min="0"
                       max={dates.length - 1}
                       onChange={e=>this.setDate(dates[e.target.value])}
                       type="range"
                       value={this.state.date === undefined || this.state.dates  === undefined ?
                              1 :
                              this.state.dates.indexOf(this.state.date) + 1}
                     />;
            
            if(this.state.date === undefined && dates.length > 0) {
                this.state.date = dates[0];
            }
        }

        let charts = "";
        if(this.state.variable === "country") {
            let id="country";
            let data = this.state.data !== undefined ?
                this.state.data["interest_by_countries_and_quarter"] :
                undefined;
            let date = this.state.date;
            let colourScale = this.state.countryColourScale;
            
            charts = (
                <div>
                  <CountryLineChart
                    id="country"
                    data={data}
                    date={date}
                    colourScale={colourScale}
                    topCategories={this.state.data !== undefined ?
                           this.state.data["top_countries"]:
                           undefined}
                  />
                  <CountryBarRace
                    id="country"
                    data={data}
                    date={date}
                    colourScale={colourScale}
                  />
                </div>
            );

        } else if (this.state.variable == "sector") {
            charts = (
                <div>
                  <SectorLineChart />
                  <SectorBarRace
                    id="country"
                    data={
                        this.state.data !== undefined ?
                        this.state.data["interest_by_countries_and_quarter"] :
                        undefined
                    }
                    date={this.state.date}
                    colourScale={this.state.countryColourScale}
                  />
                </div>
            );
            
        }

        
        return (
            <div>
              {charts}
              {slider}
              <div>{this.state.date ? this.state.date.toLocaleDateString() : ""}</div>
              <button
                className="btn btn-primary"
                onClick={this.play}
                style={{marginTop: 10}}>
                Play
              </button>
            </div>
        );
    }
}


class BarRace extends React.Component {

    constructor(props) {
        super(props);
        this.state = {};
    }

    componentDidMount() {
        console.log("BarRace.componentDidMount");
        this.container = {
            element: d3.select(`#${this.variable}`),
            height: 300,
        };

        this.container.width = parseInt(
            this.container.element.style("width"),
            10
        );
        this.canvas = {
            element: this.container.element.select(".canvas"),
            padding: {left: 50, right: 30, bottom: 15, top: 50},
            width: this.container.width,
            height: this.container.height
        };
        this.canvas.element.attr("width", this.canvas.width);
        this.canvas.element.attr("height", this.canvas.height);
        
        this.plotArea = {
            element: this.canvas.element.select(".plot-area"),
            height: this.canvas.height - this.canvas.padding.bottom - this.canvas.padding.top,
            width: this.canvas.width - this.canvas.padding.left - this.canvas.padding.right,
            x: this.canvas.padding.left,
            y: this.canvas.padding.top,
        };
        this.plotArea.element.attr(
            "transform",
            `translate(${this.plotArea.x}, ${this.plotArea.y})`
        );
        
        this.xAxis = {
            element: this.plotArea.element.select(".x-axis"),
            scale: d3.scaleLinear(),
            width: this.plotArea.width,
            x: 0,
            y: 0
            
        };
        this.xAxis.element.attr(
            "transform",
            `translate(${this.xAxis.x}, ${this.xAxis.y})`
        );
        this.xAxis.scale.range([0, this.xAxis.width]);

        this.yAxis = {
            element: this.plotArea.element.select(".y-axis"),
            scale: d3.scaleBand(),
            height: this.plotArea.height,
            x: 0,
            y: 0
            
        };
        this.yAxis.element.attr(
            "transform",
            `translate(${this.yAxis.x}, ${this.yAxis.y})`
        );
        this.yAxis.scale.range([0, this.yAxis.height]);
    }
    
    componentDidUpdate() {
        console.log("BarRace.componentDidUpdate");
        let data = this.props.data;
        if (data !== undefined) {
            this.draw();
        }
    }
    
    draw() {
        console.log("BarRace.draw()");
        let data = this.props.data;
        let nTopRanks = 10;
        let ranks = [];
        for(let i=1; i<=nTopRanks; i++){
            ranks.push(i);
        }

        let dataDate = data.filter(d=>d.quarter.toISOString() == this.props.date.toISOString());
        
        let totalInterest = dataDate.reduce(
            (acc, d)=>{
                acc = acc + d.nInterests;
                return acc;
            },
            0
        );
        let dataNormalised = [...dataDate];
        dataNormalised = dataNormalised.sort((a, b)=>b.nInterests - a.nInterests);
        for(let i=0; i<dataNormalised.length; i++) {
            dataNormalised[i] = {
                ...dataNormalised[i],
                shareOfInterest: dataNormalised[i].nInterests/totalInterest,
                rank: i+1
            };
        }

        let maxShareOfInterest = d3.max(dataNormalised, d=>d.shareOfInterest);

        this.xAxis.scale.domain([0, maxShareOfInterest]);
        this.yAxis.scale.domain(ranks);
        this.xAxis.element
            .transition()
            .call(
                d3.axisTop(this.xAxis.scale)
                    .tickFormat(d=>`${parseInt(10000*d)/100} %`)
            );
        this.yAxis.element.transition().call(d3.axisLeft(this.yAxis.scale));
        
        this.plotArea.element.selectAll(".bar")
            .data(dataNormalised, d=>d[this.variable])
            .join("rect")
            .attr("class", "bar")
            .transition()
            .attr("y", d=>d.rank > nTopRanks ? this.container.height : this.yAxis.scale(d.rank))
            .attr("width", d=>this.xAxis.scale(d.shareOfInterest))
            .attr("height", this.yAxis.scale.bandwidth() - 1)
            .attr("x", 0)
            .style("fill", d=>this.props.colourScale(d[this.variable]))
            .attr("rx", 3);
        
        this.plotArea.element.selectAll(`.${this.variable}-tag`)
            .data(dataNormalised, d=>d[this.variable])
            .join("text")
            .attr("class", `${this.variable}-tag`)
            .attr("x", this.plotArea.width)
            .attr("text-anchor", "end")
            .attr("dominant-baseline", "middle")
            .html(d=>d[this.variable])
            .transition()
            .attr(
                "y",
                d=>{
                    return d.rank > 10 ?
                        this.container.height :
                        this.yAxis.scale(d.rank) + this.yAxis.scale.bandwidth() / 2;
                }
            );
    }
    
    render() {

        console.log("BarRace.render()");
        return (
            <div className="container bar-race" id={`${this.variable}`}>
              <svg className="canvas">
                <g className="plot-area">
                  <g className="x-axis"></g>
                  <g className="y-axis"></g>
                </g>
              </svg>
              <div className="tooltip"></div>
            </div>
        );
    }


}

class LineChart extends React.Component {

    constructor(props) {
        super(props);
        this.state = {};
    }

    componentDidMount() {
        console.log("LineChart.componentDidMount");
        this.container = {
            element: d3.select(`#${this.variable}-line-chart`),
            height: 300,
        };

        this.container.width = parseInt(
            this.container.element.style("width"),
            10
        );
        this.canvas = {
            element: this.container.element.select(".canvas"),
            padding: {left: 50, right: 30, bottom: 50, top: 15},
            width: this.container.width,
            height: this.container.height
        };
        this.canvas.element.attr("width", this.canvas.width);
        this.canvas.element.attr("height", this.canvas.height);
        
        this.plotArea = {
            element: this.canvas.element.select(".plot-area"),
            height: this.canvas.height - this.canvas.padding.bottom - this.canvas.padding.top,
            width: this.canvas.width - this.canvas.padding.left - this.canvas.padding.right,
            x: this.canvas.padding.left,
            y: this.canvas.padding.top,
        };
        this.plotArea.element.attr(
            "transform",
            `translate(${this.plotArea.x}, ${this.plotArea.y})`
        );
        
        this.xAxis = {
            element: this.plotArea.element.select(".x-axis"),
            scale: d3.scaleTime(),
            width: this.plotArea.width,
            x: 0,
            y: this.plotArea.height,
            
        };
        this.xAxis.element.attr(
            "transform",
            `translate(${this.xAxis.x}, ${this.xAxis.y})`
        );
        this.xAxis.scale.range([0, this.xAxis.width]);

        this.yAxis = {
            element: this.plotArea.element.select(".y-axis"),
            scale: d3.scaleLinear(),
            height: this.plotArea.height,
            x: 0,
            y: 0
            
        };
        this.yAxis.element.attr(
            "transform",
            `translate(${this.yAxis.x}, ${this.yAxis.y})`
        );
        this.yAxis.scale.range([this.yAxis.height, 0]);

        this.layer0 = {
            element: this.plotArea.element.select(".layer-0"),
        };

        this.coverUp = {
            element:this.plotArea.element.select(".cover-up"),
            width: this.plotArea.width + 2,  // hide stroke of line
            height: this.plotArea.height
        };
        
        this.coverUp.element
            .attr("x", 1) // uncover axis
            .attr("width", this.coverUp.width)
            .attr("height", this.coverUp.height + 2) // line stroke
            .style("fill", "white");

        this.legend = {
            element: this.plotArea.element.select(".legend"),
            x: 0,
            y: 0
        };
        this.legend.element
            .attr("transform", `translate(${this.legend.x}, ${this.legend.y})`);

    }
    
    componentDidUpdate() {
        console.log("LineChart.componentDidUpdate");
        console.log(this.props.date);

        if(this.props.data !== undefined) {
            if(this.plotArea.element.selectAll(".line").size() == 0) {
                this.draw();
            }
        }
        
        if (this.props.data !== undefined && this.props.date !== undefined) {
            let x = this.xAxis.scale(this.props.date);
            this.coverUp.element
                .transition()
                .attr("x", x)
                .attr("width", this.plotArea.width - x);
        }
    }
    
    draw() {
        console.log("LineChart.draw()");
        let data = this.props.data;
        let top10 = this.props.topCategories.map(d=>d[this.variable]);
        top10 = top10.splice(0, 10);
        data = data.filter(d=>top10.indexOf(d[this.variable]) != -1);

        let startDate = d3.min(data, d=>d.quarter);
        let endDate = d3.max(data, d=>d.quarter);
        
        this.xAxis.scale.domain([startDate, endDate]);
        this.xAxis.element.transition().call(d3.axisBottom(this.xAxis.scale));

        let minNInterests = d3.min(data, d=>d.nInterests);
        let maxNInterests = d3.max(data, d=>d.nInterests);
        this.yAxis.scale.domain([minNInterests, maxNInterests]);
        this.yAxis.element.transition().call(d3.axisLeft(this.yAxis.scale));

        let plotLine = d3.line()
            .x(d => this.xAxis.scale(d.quarter))
            .y(d => this.yAxis.scale(d.nInterests))
            .curve(d3.curveMonotoneX);

        let groupedData = data.reduce(
            (acc, d) => {
                if(acc[[d[this.variable]]] === undefined) {
                    acc[[d[this.variable]]] = {
                        variable: d[this.variable],
                        values: []
                    };                    
                }
                acc[[d[this.variable]]].values.push(d);
                return acc;
            },
            {}
        );
        groupedData = Object.values(groupedData);

        this.layer0.element.selectAll(".line")
            .data(groupedData, d=>d.variable)
            .join("path")
            .attr("class", "line")
            .attr("d", d=>plotLine(d.values))
            .style("stroke", d=>this.props.colourScale(d.variable));


        let legendItems = this.legend.element.selectAll(".legend-item")
            .data(groupedData, d=>d[this.variable])
            .join("g")
            .attr("class", "legend-item");

        let legendBlockWidth = 20;
        legendItems
            .data(groupedData)
            .append("rect")
            .attr("x", 10)
            .attr("y", (d,i)=>i * legendBlockWidth + 1)
            .attr("width", legendBlockWidth)
            .attr("height", legendBlockWidth)
            .style("fill", d=>this.props.colourScale(d.variable));

        legendItems
            .data(groupedData)
            .append("text")
            .attr("x", 10 + legendBlockWidth + 10)
            .attr("y", (d, i)=>i * legendBlockWidth + 1 + (legendBlockWidth / 2))
            .html(d=>d.variable)
            .attr("text-anchor", "start")
            .attr("dominant-baseline", "middle");
        
    }
    
    render() {
        console.log("Line.render()");
        return (
            <div className="container line-chart" id={`${this.variable}-line-chart`}>
            <svg className="canvas">
              <g className="plot-area">
                <g className="layer-0"></g>            
                <rect className="cover-up" />
                <g className="x-axis"></g>
                <g className="y-axis"></g>
                <g className="legend"></g>
              </g>
            </svg>
              <div className="tooltip"></div>
            </div>
        );
    }
}

class CountryBarRace extends BarRace {

    constructor(props) {
        super(props);
        this.variable = "country";
    }
    
}

class SectorBarRace extends BarRace {

    constructor(props) {
        super(props);
        this.variable = "sector";
    }
    
}

class CountryLineChart extends LineChart {

    constructor(props) {
        super(props);
        this.variable = "country";
    }

    
}

class SectorLineChart extends LineChart {

    constructor(props) {
        super(props);
        this.variable = "sector";
    }
    
}

render(<App/>, document.getElementById('react-container'));

