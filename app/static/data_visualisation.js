console.log("data_visualisation");

import React from "react";
import axios from "axios";
import * as d3 from "d3";
import { render } from "react-dom";

window.React = React;

class App extends React.Component {


    componentDidMount() {
        this.getDataset();
    }

    componentDidUpdate() {
    }

    constructor(props){
        super(props);
        this.state = {
	    barRaceVariable: "nInterestsCumulative", //"shareOfInterest",
	    cumulative: true,
            exporterStatus: "interested",
            intervals: [],
            loading: true,
            nTop: 10,
            playing: false,
            groupby: "country", // "sector"
        };

        this.play = this.play.bind(this);
        this.setBarRaceVariable = this.setBarRaceVariable.bind(this);
        this.setData = this.setData.bind(this);
        this.setDate = this.setDate.bind(this);
        this.setExporterStatus = this.setExporterStatus.bind(this);
        this.setGroupby = this.setGroupby.bind(this);
        this.setNextDate = this.setNextDate.bind(this);
        this.setPlaying = this.setPlaying.bind(this);
        this.togglePlaying = this.togglePlaying.bind(this);
    }

    getDataset() {
        console.log("App.getDataset()");
        console.log(this.state);
        let url = this.getDatasetUrl();
        this.setState({loading: true});
        axios.get(url)
            .then(response => this.setData(response.data))
            .catch(response => alert(`failed to get data. ${response}`))
            .finally(()=>this.setState({loading: false}));
        
    }

    getDatasetUrl() {
        let url = `/api/v1/get-data-visualisation-data/${this.state.groupby}?`;
        url += `exporter-status=${this.state.exporterStatus}`;
        return url;
    }
    
    play() {
        console.log("App.play");
        return window.setInterval(this.setNextDate, 1000);
    }

    setBarRaceVariable(variable) {
	this.setState({barRaceVariable: variable});
    }
    
    setData(data) {
	console.log("App.setData");
	console.log(data);

        // convert datestrings to date objects
        let processedData = {
            ...data,
            nInterests: data.nInterests.map(
                d=>({...d, date: new Date(d.date)})
            )
        };

        let dates = Object.values(
            processedData.nInterests.reduce(
                (acc, d)=> ({...acc, [d.date]: d.date}),
                {}
            )
        );
        dates = dates.sort((a, b)=>a - b);

        let date = dates.length > 0 ? dates[0] : undefined;

        let groups = processedData.top;

        let groupColourScale = d3.scaleOrdinal()
            .domain(groups)
            .range(d3.schemeTableau10);
        
        this.setState(
            {
                data: processedData,
                date: date,
                dates: dates,
                groups: groups,
                groupColourScale: groupColourScale,
            }
        );
    }

    setDate(date) {
        console.log("setDate");
        console.log(`date: ${date}`);
        let index = this.state.dates.indexOf(date);
        if(index !== -1) {
            this.setState({date: date});
        }
        let lastDate = this.state.dates[this.state.dates.length - 1];
        if(date === undefined || date === lastDate) {
            this.setPlaying(false);
        }
        
    }

    setExporterStatus(status) {
        this.setState({exporterStatus: status, loading: true}, this.getDataset);
    }

    setGroupby(groupby) {
        this.setState({groupby: groupby, loading: true}, this.getDataset);
        ;
    }

    setNextDate() {
        let date = this.state.date;
        let index = this.state.dates.indexOf(date);
        if(index !== -1) {
            let nextDate = this.state.dates[index+1];
            this.setDate(nextDate);
        }
    }

    setPlaying(playing)  {
        for(let i=0; i<this.state.intervals.length; i++){
            let interval = this.state.intervals.pop();
            window.clearInterval(interval);
        }
        
        let intervals = [];
        if(playing === true){
            let interval = this.play();
            intervals = [interval];
        } 
        this.setState({playing: playing, intervals: intervals});
    }

    toCamelCase(d) {
        let length = d.length;
        let output = "";
        let index = 0;
        let previousChar = null;
        
        while(index != length) {
            let char = d[index];
            if (previousChar === "_" && char !== "_") {
                if(output.length === 0) {
                    output += char.toLowerCase();
                } else {
                    output += char.toUpperCase();
                }
            } else if (previousChar !== "_" && char !== "_") {
                output += char.toLowerCase();
            }
            previousChar = char;
            index += 1;
        }
        
        return output;
        
    }
    
    togglePlaying() {
        let playing = !(this.state.playing);
        this.setPlaying(playing);
    }
    
    render() {
        console.log("App.render");
        console.log(this.state);

        let dates = this.state.dates;

        let charts = "";
        let data = this.state.data;
        let date = this.state.date;
        let colourScale = this.state.groupColourScale;
        let variable = this.state.barRaceVariable;


        let slider = (
            <div style={
                {
                    marginLeft: 45,
                    marginRight: 10,
                    marginTop: "1em",
                    marginBottom: "1em"}
            }
            >
              <input
                className="custom-range"
                min={0}
                max={dates !== undefined ? dates.length - 1 : 0}
                onChange={e=>this.setDate(dates[e.target.value])}
                type="range"
                value={
                    this.state.date === undefined
                        || this.state.dates  === undefined
                        ? 0
                        : this.state.dates.indexOf(this.state.date)}
              />
	      <div style={{textAlign: "right"}}>
                {this.state.date ? this.state.date.toLocaleDateString() : ""}
              </div>
            </div>
        );
        
        charts = (
            <div>
              <LineChart
                groupby={this.toCamelCase(this.state.groupby)}
                colourScale={colourScale}
                data={data}
                date={date}
                id="line-chart"
                loading={this.state.loading}
                nTop={this.state.nTop}
                variable={variable}
              />
              <BarRace
                groupby={this.toCamelCase(this.state.groupby)}
                colourScale={colourScale}
                data={data}
                date={date}
                id="bar-race"
                loading={this.state.loading}
                nTop={this.state.nTop}
                variable={variable}
              />

              {slider}
            </div>
        );

        let loadingBar = (
            <div
              className="progress-bar progress-bar-striped"
              style={{position: "fixed", top: 0, height: 30, width:"100%"}}>
              Loading...
            </div>
        );

        let title = "";
        if(this.state.groupby === "country" && this.state.loading === false){
            if(this.state.exporterStatus === "interested"){
                title = "Countries of interest";
            } else if (this.state.exporterStatus === "mentioned") {
                title = "Countries mentioned in interactions";
            }
        } else if (this.state.groupby === "sector" && this.state.loading === false) {
            title = "Sectors of interest";
        }
        title = <h2 style={{marginBottom: "1em", marginTop: 40}}>{title}</h2>;

        let playButton = (
            <div className="form-group row">
              <label className="col-md-3 col-form-label text-right"></label>
	      <button
                className="btn btn-primary col-md-9"
                onClick={this.togglePlaying}
                style={{marginTop: 10}}>
                {this.state.playing === true ? "Stop" : "Play"}
              </button>
            </div>
        );

        let groupbySelector = (
            <div className="form-group row">
              <label className="col-md-3 col-form-label text-right">Groupby</label>
              <select
                className="custom-select col-md-9"
                onChange={(e)=>this.setGroupby(e.target.value)}
                value={this.state.groupby}
              >
                <option value="country">
                  Country
                </option>
                <option
                  value="sector"
                  disabled={this.state.exporterStatus === "mentioned"}>
                  Sector
                </option>
              </select>
              
            </div>
        );

        let options = [
            {value: "interested", html: "Interested"},
            {value: "mentioned", html: "Mentioned in interactions"}
        ];
        if(this.state.groupby == "sector") {
            options = options.filter(o=>o.value !== "mentioned");
        }

        let exporterStatusSelector = (
            <div className="form-group row">
              <label className="col-md-3 col-form-label text-right">
                Exporter status
              </label>
              <select
                className="custom-select col-md-9"
                onChange={(e)=>this.setExporterStatus(e.target.value)}
                value={this.state.exporterStatus}
              >
                {
                    options.map(
                        (option, i) => {
                            return (
                                <option
                                  key={i}
                                  value={option.value}
                                >
                                  {option.html}
                                </option>
                            );
                        }
                    )
                }
              </select>
            </div>
        );
        
        let barRaceVariableSelector = (
            <div className="form-group row">
              <label className="col-md-3 col-form-label text-right">Variable</label>
              <select
                className="custom-select col-md-9"
                onChange={(e)=>this.setBarRaceVariable(e.target.value)}
                value={this.state.barRaceVariable}
              >
                <option value="nInterests">
                  Number of Interests
                </option>
                <option value="nInterestsCumulative">
                  Cumulative number of interests
                </option>
                <option value="shareOfInterest">
                  Share of interest on date
                </option>
                <option value="shareOfInterestCumulative">
                  Cumulative Share of interest
                </option>
              </select>
              
            </div>
        );
        
        return (
            <div>
              {this.state.loading ? loadingBar : ""}
	      <div className="container">
                {title}
	        {charts}
                <div className="container" style={{marginTop: "1em"}}>
                  {playButton}
                  {groupbySelector}
                  {exporterStatusSelector}
                  {barRaceVariableSelector}
                </div>            
              </div>
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
            element: d3.select(`#${this.groupby}`),
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
        // this.canvas.element.attr("height", this.canvas.height);
        
        this.plotArea = {
            element: this.canvas.element.select(".plot-area"),
            height: this.canvas.height
                - this.canvas.padding.bottom
                - this.canvas.padding.top,
            width: this.canvas.width
                - this.canvas.padding.left
                - this.canvas.padding.right,
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

        if(this.props.loading === true) {
            this.canvas.element.transition().duration(1000).attr("height", 0);
        } else {
            this.canvas
                .element
                .transition()
                .duration(1000)
                .attr("height", this.canvas.height);
        };
        
        let data = this.props.data;
        if (data !== undefined && this.props.loading === false) {
            this.draw();
        }
    }
    
    draw() {
        console.log("BarRace.draw()");
        
        let data = this.props.data.nInterests;
        let nTop = this.props.nTop;
        let variable = this.props.variable;

        let dataDate = data.filter(
            d=>d.date.toISOString() == this.props.date.toISOString()
        );
        dataDate = dataDate.filter(d=>d[variable] > 0);
        dataDate = dataDate.sort((a, b)=>b[variable] - a[variable]);
        dataDate = dataDate.map((d, i)=>({...d, rank: i}));
        dataDate = dataDate.splice(0, nTop);

        console.log(dataDate);
        
        let xMin = 0;
        let xMax = d3.max(dataDate, d=>d[this.props.variable]);
        this.xAxis.scale.domain([xMin, xMax]);
        this.xAxis.element
            .transition()
            .duration(1000)
            .call(
                this.props.variable === "shareOfInterest"
                    || this.props.variable === "shareOfInterestCumulative"
                    ? d3.axisTop(this.xAxis.scale)
                    .tickFormat(d=>`${parseInt(10000*d)/100} %`)
                    : d3.axisTop(this.xAxis.scale)
            );

        let ranks = [];
        for(let i=0; i<nTop; i++){
            ranks.push(i);
        }
        this.yAxis.scale.domain(ranks);
        this.yAxis.element.transition().call(d3.axisLeft(this.yAxis.scale));

        let groups = this.plotArea.element.selectAll(".group")
            .data(dataDate, d=>d[this.props.groupby]);

        let newGroups = groups
            .enter()
            .append("g")
            .attr("class", "group");

        newGroups
            .attr("transform", `translate(0, ${this.canvas.height})`)
            .transition()
            .duration(1000)
            .attr("transform", d=> `translate(0, ${this.yAxis.scale(d.rank)})`);

        newGroups
            .append("rect")
            .attr("class", "bar")
            .attr("width", d=>this.xAxis.scale(d[variable]))
            .attr("height", this.yAxis.scale.bandwidth() - 1)
            .style("fill", d=>this.props.colourScale(d[this.props.groupby]))
            .attr("rx", 3);
        
        newGroups
            .append("text")
            .attr("x", this.plotArea.width - 10)
            .attr("y", this.yAxis.scale.bandwidth() / 2 + 1)
            .attr("text-anchor", "end")
            .attr("dominant-baseline", "middle")
            .html(d=>d[this.props.groupby]);

        groups
            .transition()
            .duration(1000)
            .attr("transform", d=> `translate(0, ${this.yAxis.scale(d.rank)})`);
        
        groups.selectAll(".bar")
            .data(dataDate, d=>d[this.props.groupby])
            .transition()
            .duration(1000)
            .attr("width", d=>this.xAxis.scale(d[variable]));

        groups
            .exit()
            .transition()
            .duration(1000)
            .attr("transform", `translate(0, ${this.canvas.height})`)
            .remove();
        
    }
    
    render() {

        console.log("BarRace.render()");
        return (
            <div className="container bar-race" id={`${this.groupby}`}>
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
        this.container = {
            id: "line-chart",
            height: 300,
        };
    }

    componentDidMount() {
        console.log("LineChart.componentDidMount");
        this.container.element = d3.select(`#${this.container.id}`),
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
        // this.canvas.element.attr("height", this.canvas.height);
        
        this.plotArea = {
            element: this.canvas.element.select(".plot-area"),
            height: this.canvas.height
                - this.canvas.padding.bottom
                - this.canvas.padding.top,
            width: this.canvas.width
                - this.canvas.padding.left
                - this.canvas.padding.right,
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
            height: this.plotArea.height + 2
        };
        
        this.coverUp.element
            .attr("x", 1) // uncover axis
            .attr("y", -2) // plot stroke
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
        console.log(this.props);

        if(this.props.loading === true) {
            this.canvas.element.transition().duration(1000).attr("height", 0);
        } else {
            this.canvas
                .element
                .transition()
                .duration(1000)
                .attr("height", this.canvas.height);
        };

        if(this.props.data !== undefined && this.props.loading === false) {
            this.draw();
        }
        
        if (this.props.data !== undefined
            && this.props.date !== undefined
            && this.props.loading === false) {
            let x = this.xAxis.scale(this.props.date);
            let width = this.plotArea.width - x;
            if(x !== undefined && !isNaN(x)) {
                this.coverUp.element
                    .transition()
                    .ease(d3.easeLinear)
                    .duration(1000)
                    .attr("x", x)
                    .attr("width", width);
            }
        }
    }
    
    draw() {
        console.log("LineChart.draw()");
        console.log(this.props);
        
        let data = this.props.data;
        let nInterests = data.nInterests;
        let nTop = this.props.nTop;
        let top = data.top;
        let topGroups = top.slice(0, nTop);
        nInterests = nInterests.filter(
            d=>topGroups.indexOf(d[this.props.groupby]) !== -1
        );

        let startDate = d3.min(nInterests, d=>d.date);
        let endDate = d3.max(nInterests, d=>d.date);
        this.xAxis.scale.domain([startDate, endDate]);
        this.xAxis.element.call(d3.axisBottom(this.xAxis.scale));

        let minY = 0;
        let maxY = d3.max(nInterests, d=>d[this.props.variable]);
        this.yAxis.scale.domain([minY, maxY]);
        this.yAxis.element.transition().call(d3.axisLeft(this.yAxis.scale));

        let plotLine = d3.line()
            .x(d => this.xAxis.scale(d.date))
            .y(d => this.yAxis.scale(d[this.props.variable]))
            .curve(d3.curveMonotoneX);

        let groupedData = nInterests.reduce(
            (acc, d) => {
                if(acc[[d[this.props.groupby]]] === undefined) {
                    acc[[d[this.props.groupby]]] = {
                        group: d[this.props.groupby],
                        values: []
                    };                    
                }
                acc[[d[this.props.groupby]]].values.push(d);
                return acc;
            },
            {}
        );
        groupedData = Object.values(groupedData);

        this.layer0.element.selectAll(".line")
            .data(groupedData, d=>d.group)
            .join("path")
            .attr("class", "line")
            .style("stroke", d=>this.props.colourScale(d.group))
            .transition()
            .attr("d", d=>plotLine(d.values));

        let legendBlockWidth = 20;
        
        let legendItems = this.legend.element.selectAll(".legend-item")
            .data(groupedData, d=>d.group);

        let newLegendItems = legendItems
            .enter()
            .append("g")
            .attr("class", "legend-item")
	    .attr("transform", (d, i)=>`translate(0, ${i * legendBlockWidth + 1})`);

        newLegendItems
            .append("rect")
            .attr("class", "legend-block")
            .attr("x", 10)
            .attr("rx", 3)        
            .attr("width", legendBlockWidth - 2)
            .attr("height", legendBlockWidth - 2)
            .style("fill", d=>this.props.colourScale(d.group));
        
        newLegendItems
            .append("text")
            .attr("class", "legend-text")
            .attr("x", 10 + legendBlockWidth + 10)
            .attr("y", legendBlockWidth / 2)
            .html(d=>d.group)
            .attr("text-anchor", "start")
            .attr("dominant-baseline", "middle");

        legendItems.
            transition()
            .attr("transform", (d, i)=>`translate(0, ${i * legendBlockWidth + 1})`);

        legendItems
            .selectAll("rect")
            .data(groupedData, d=>d.group)
            .transition()
            .style("fill", d=>this.props.colourScale(d.group));        

        legendItems.exit().remove();
        
    }
    
    render() {
        console.log("LineChart.render()");
        return (
            <div className="container line-chart" id={`${this.container.id}`}>
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

render(<App/>, document.getElementById('react-container'));

