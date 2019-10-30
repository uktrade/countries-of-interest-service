import React from 'react';
import axios from 'axios';
import * as d3 from "d3";
import { render } from 'react-dom';

window.React = React;

class App extends React.Component {

    constructor(props) {
        super(props);
        this.orderFrequencyChartId = 'order-frequency-chart';
        this.state = {
            data: null,
            nRowsTopSector: 5
        };
        this.drawOrderFrequencyChart = this.drawOrderFrequencyChart.bind(this);
        this.onNRowsTopSectorChange = this.onNRowsTopSectorChange.bind(this);
    }

    componentDidMount() {
        axios.get('/api/v1/get-data-report-data')
            .then(response => response.data)
            .then(data => this.setState({data: data}))
            .then(x => this.drawOrderFrequencyChart());
    }

    drawOrderFrequencyChart() {
        if (this.state.data == null){
            return
        }
        const data = this.state.data.omisOrderFrequency.values;
        const dates = data.map(row=>new Date(row[0]));
        const n_orders = data.map(row=>row[1]);
        const parsedData = d3.zip(dates, n_orders);

        let container = d3.select(`#${this.orderFrequencyChartId}`);
        let canvas = container.select(".canvas");
        let canvasWidth = parseInt(container.style('width'), 10);
        let canvasHeight = 300;
        canvas.attr("width", canvasWidth)
            .attr("height", canvasHeight);
        
        let chart = canvas.select(".chart");
        let chartMargins = {
            left: 50,
            right: 30,
            bottom: 50,
            top: 15
        };
        let chartWidth = canvasWidth - chartMargins.left - chartMargins.right;
        let chartHeight = canvasHeight - chartMargins.top - chartMargins.bottom;
        chart.attr(
            'transform',
            `translate(${chartMargins.left}, ${chartMargins.top})`
        );

        
        let xScale = d3.scaleTime()
            .range([0, chartWidth])
            .domain([d3.min(dates), d3.max(dates)]);

        let yScale = d3.scaleLinear()
            .range([chartHeight, 0])
            .domain([d3.min(n_orders), d3.max(n_orders)]);

        chart.selectAll("*").remove();

        var valueline = d3.line()
            .x(function(d) { return xScale(d[0]); })
            .y(function(d) { return yScale(d[1]);  })
            .curve(d3.curveMonotoneX);

        chart.append("path")
            .data([parsedData]) 
            .attr("class", "line")  
            .attr("d", valueline);
        
        let xAxis = d3.axisBottom(xScale);
        chart.append('g')
            .attr('transform', `translate(0, ${chartHeight})`)
            .call(xAxis);

        let yAxis = d3.axisLeft(yScale);
        chart.append('g')
            .call(yAxis);

        chart.selectAll(".dot")
            .data(parsedData)
            .enter()
            .append("circle") 
            .attr("class", "dot") 
            .attr("cx", function(d) { return xScale(d[0]); })
            .attr("cy", function(d) { return yScale(d[1]); })
            .attr("r", 2);  

    }
    
    onNRowsTopSectorChange(nRows){
        this.setState({nRowsTopSector: nRows});
    }
    
    render() {
        console.log("render");
        console.log(this.state);

        const {data, nRowsTopSector, orderFrequency} = this.state;
        if (data == null){
            return '';
        }
        
        const {
            nCompanies,
            nCompaniesMatchedToCompaniesHouse,
            nCompaniesMatchedToSector,
            nCompaniesMatchedToDuplicateCompaniesHouse,
            nSectors,
            nCompaniesWithOmisOrders,
            nCompaniesWithExportCountries,
            nCompaniesWithFutureInterestCountries,
        } = data;
        
        const chartId = this.orderFrequencyChartId;
        return (
            <div style={{paddingTop: '1em'}}>
              <MatchedCompanies
                nCompanies={nCompanies}
                nDuplicates={nCompaniesMatchedToDuplicateCompaniesHouse}
                nMatches={nCompaniesMatchedToCompaniesHouse}
              />
              <SectorMatches
                nCompanies={nCompanies}
                nMatches={nCompaniesMatchedToSector}
                nSectors={nSectors}
              />
              <TopSectors
                data={data.topSectors.values}
                onChange={this.onNRowsTopSectorChange}
                nRows={nRowsTopSector}
              />
              <CompanyOrderSummary nCompanies={nCompanies} nWithOrders={nCompaniesWithOmisOrders}/>
              <CompanyExportCountriesSummary
                nCompanies={nCompanies}
                nWithExportCountries={nCompaniesWithExportCountries}
              />
              <CompanyCountriesOfInterestSummary
                nCompanies={nCompanies}
                nWithCountriesOfInterest={nCompaniesWithFutureInterestCountries}
              />
              <OrderFrequency
                chartId={chartId}
                data={data.omisOrderFrequency.values}
              />
            </div>
        );

    }
}

const MatchedCompanies = ({nCompanies, nDuplicates, nMatches}) => {
    if(nCompanies == null){
        return '';
    }

    let nUniqueMatches = nMatches - nDuplicates;
    
    return (
        
        <div className="data-report">
          <h3 style={{paddingBottom: '0.5em'}}>Datahub - Companies House matching</h3>
          <table className="table table-striped">
            <thead>
              <tr>
                <th></th>
                <th>Count</th>
                <th>Percent</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Companies</td>
                <td>{nCompanies}</td>
                <td></td>
              </tr>
              <tr>
                <td>Matches</td>
                <td>{nMatches}</td>
                <td>{Math.round(100 * 100 * nMatches/nCompanies)/100}</td>
              </tr>
            </tbody>
          </table>

          <h3 style={{paddingBottom: '0.5em'}}>Breakdown of matched companies</h3>
          <table className="table table-striped">
            <thead>
              <tr>
                <th></th>
                <th>Count</th>
                <th>Percent</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Matches</td>
                <td>{nMatches}</td>
                <td></td>
              </tr>
              <tr>
                <td>Unique Matches</td>
                <td>{nUniqueMatches}</td>
                <td>{Math.round(100 * 100 * nUniqueMatches / nMatches) / 100}</td>
              </tr>
              <tr>
                <td>Duplicates</td>
                <td>{nDuplicates}</td>
                <td>{Math.round(100 * 100 * nDuplicates / nMatches) / 100}</td>
              </tr>
            </tbody>
          </table>
        </div>
        
    );
};

const SectorMatches = ({nCompanies, nSectors, nMatches}) => {

    if(nCompanies == null){
        return '';
    }
    
    return (
        <div className="sector-summary">
          <h3>Sector summary</h3>
          <table className="table table-striped">
            <tbody>
              <tr>
                <td>#Companies</td><td>{nCompanies}</td>
              </tr>
              <tr>
                <td>#Sectors</td><td>{nSectors}</td>
              </tr>
              <tr>
                <td>#Companies matched to a sector</td><td>{nMatches}</td>
              </tr>
              <tr>
                <td>%Companies matched to a sector</td>
                <td>{Math.round(100 * 100 * nMatches/nCompanies)/100}</td>
              </tr>
            </tbody>
          </table>
        </div>
    );
};

const TopSectors = ({data, onChange, nRows=5}) => {
    let nRowsInput;
    const onChange2 = (e) => {
        return onChange(nRowsInput.value);
    };
    const filteredData = data.slice(0, nRows);
    return (
        <div>
          <h3>Top sectors</h3>
          <div className="form-group">
            <label className="form-label">Number of rows</label>
            <input
              ref={input => nRowsInput = input}
              onChange={onChange2}
              className="form-control"
              type="number"
              value={nRows}
            />
          </div>
          <table className="table table-striped">
            <thead>
              <tr>
                <th>Sector</th>
                <th>Count</th>
              </tr>
            </thead>
            <tbody>
              {
                  filteredData.map((sector, i) => {
                      return (
                          <tr key={i}>
                            <td>{sector[0]}</td>
                            <td>{sector[1]}</td>
                          </tr>
                      );
                  })
              }
            </tbody>
          </table>
        </div>        
    );
};

const CompanyOrderSummary = ({nCompanies, nWithOrders}) => {
    return (
        <div className="company-order-summary">
          <h3>Company order summary</h3>
          <table className="table table-striped">
            <tbody>
              <tr><td>#Companies</td><td>{nCompanies}</td></tr>
              <tr><td>#Companies with orders</td><td>{nWithOrders}</td></tr>
              <tr>
                <td>%Companies with orders</td>
                <td>{Math.round(100 * 100 * nWithOrders/nCompanies)/100}</td>
              </tr>
            </tbody>
          </table>
        </div>
    );

};

const CompanyExportCountriesSummary = ({nCompanies, nWithExportCountries}) => {
    if(nCompanies == null){
        return '';
    }
    
    return (
        <div className="company-export-country-summary">
          <h3>Company export country summary</h3>
          <table className="table table-striped">
            <tbody>
              <tr><td>#Companies</td><td>{nCompanies}</td></tr>
              <tr><td>#Companies with export countries</td><td>{nWithExportCountries}</td></tr>
              <tr>
                <td>%Companies with export countries</td>
                <td>{Math.round(100 * 100 * nWithExportCountries/nCompanies)/100}</td>
              </tr>
            </tbody>
          </table>
        </div>
    );

};

const CompanyCountriesOfInterestSummary = ({nCompanies, nWithCountriesOfInterest}) => {
    if(nCompanies == null){
        return '';
    }

    return (
        <div className="company-countries-of-interest-summary">
          <h3>Countries of interest summary</h3>
          <table className="table table-striped">
            <tbody>
              <tr><td>#Companies</td><td>{nCompanies}</td></tr>
              <tr>
                <td>#Companies with countries of interest</td>
                <td>{nWithCountriesOfInterest}</td>
              </tr>
              <tr>
                <td>%Companies with countries of interest</td>
                <td>{Math.round(100 * 100 * nWithCountriesOfInterest/nCompanies)/100}</td>
              </tr>
            </tbody>
          </table>
        </div>
    );
};

const OrderFrequency = ({chartId, data}) => {
    if(data == null){
        return '';
    }

    return (
        <div className='order-frequency'>
          <h3>Order frequency</h3>
          <OrderFrequencyChart data={data} id={chartId}/>
        </div>
    );
};

const OrderFrequencyChart = ({data, id}) => {
    if(data == null){
        return '';
    }

    return (
        <div id={id} className="order-frequency-chart">
          <svg className="canvas">
            <g className="chart">
            </g>
          </svg>
        </div>
    );
};

render(<App/>, document.getElementById('react-container'));
