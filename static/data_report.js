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
            data: {
                dataReportData: {
                    companyOrderSummary: {
                        headers: ['nCompanies', 'nCompaniesWithOrders'],
                        data: [10, 2]
                    },
                    companyExportCountriesSummary: {
                        headers: ['nCompanies', 'nCompaniesWithExportCountries'],
                        data: [10, 1]
                    },
                    countriesOfInterestSummary: {
                        headers: ['nCompanies', 'nCompaniesWithCountriesOfInterest'],
                        data: [10, 2]
                    },
                    orderFrequency: {
                        daily: {
                            headers: [],
                            data: [
                                ['2018-01-01', 1],
                                ['2018-01-02', 2],
                                ['2018-01-03', 3]
                            ]
                        },
                        weekly: {
                            headers: [],
                            data: [
                                ['2018-01-01', 4],
                                ['2018-01-08', 5],
                                ['2018-01-15', 6]
                            ]
                        },
                        monthly: {
                            headers: [],
                            data: [
                                ['2018-01-01', 4],
                                ['2018-02-01', 5],
                                ['2018-03-01', 6]
                            ]
                        }
                    }
                },
                matchedCompanies: {
                    nCompanies: 10,
                    nMatches: 3,
                    nUniqueMatches: 2,
                    nDuplicates: 1,
                    percentMatches: 30,
                    percentUniqueMatches: 20,
                    percentDuplicates: 10
                },
                sectorMatches: {
                    nCompanies: 10,
                    nMatches: 9,
                    nSectors: 302
                },
                topSectors: [
                        {name: 'Aerospace', count: 6},
                        {name: 'Defence', count: 2},
                        {name: 'Food', count: 1}
                ],
            },
            orderFrequency: "daily",
            nRowsTopSector: 5
        };
        this.drawOrderFrequencyChart = this.drawOrderFrequencyChart.bind(this);
        this.onNRowsTopSectorChange = this.onNRowsTopSectorChange.bind(this);
        this.onOrderFrequencyChange = this.onOrderFrequencyChange.bind(this);
    }

    componentDidMount() {
        axios.get('/api/get-matched-companies')
            .then(response => response.data)
            .then(
                results => this.setState(
                    prevState => {
                        const {data} = prevState;
                        const newData = {...data, matchedCompanies: results};
                        return {...prevState, data: newData};
                    }
                )
            );

        axios.get('/api/get-sector-matches')
            .then(response => response.data)
            .then(
                results => this.setState(
                    prevState => {
                        const {data} = prevState;
                        const newData = {...data, sectorMatches: results};
                        return {...prevState, data: newData};
                    }
                )
            );

        axios.get('/api/get-top-sectors')
            .then(response => response.data)
            .then(
                results => {
                    return this.setState(
                        prevState => {
                            const {data} = prevState;
                            const newData = {...data, topSectors: results.data};
                            return {...prevState, data: newData};
                        }
                    );
                }
            );

        axios.get('/api/get-data-report-data')
            .then(response => response.data)
            .then(
                results => {
                    return this.setState(
                        prevState => {
                            const {data} = prevState;
                            const newData = {...data, dataReportData: results};
                            return {...prevState, data: newData};
                        }
                    );
                }
            );
    }

    drawOrderFrequencyChart() {
        const {data} = this.state.data.dataReportData.orderFrequency[this.state.orderFrequency];
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
            .append("circle") // Uses the enter().append() method
            .attr("class", "dot") // Assign a class for styling
            .attr("cx", function(d) { return xScale(d[0]) })
            .attr("cy", function(d) { return yScale(d[1]) })
            .attr("r", 2);  

    }

    componentDidUpdate() {
        this.drawOrderFrequencyChart();
    }
    
    onNRowsTopSectorChange(nRows){
        this.setState({nRowsTopSector: nRows});
    }

    onOrderFrequencyChange(frequency){
        this.setState({orderFrequency: frequency});
    }
    
    render() {
        console.log("render");
        console.log(this.state);

        const {data, nRowsTopSector, orderFrequency} = this.state;
        const {dataReportData} = data;
        const chartId = this.orderFrequencyChartId;
        return (
            <div style={{paddingTop: '1em'}}>
              <MatchedCompanies data={data.matchedCompanies}/>
              <SectorMatches data={data.sectorMatches}/>
              <TopSectors
                data={data.topSectors}
                onChange={this.onNRowsTopSectorChange}
                nRows={nRowsTopSector}
              />
              <CompanyOrderSummary data={dataReportData.companyOrderSummary}/>
              <CompanyExportCountriesSummary data={dataReportData.companyExportCountriesSummary}/>
              <CompanyCountriesOfInterestSummary
                data={dataReportData.countriesOfInterestSummary}
              />
              <OrderFrequency
                chartId={chartId}
                data={dataReportData.orderFrequency}
                frequency={orderFrequency}
                onChange={this.onOrderFrequencyChange}
              />
            </div>
        );

    }
}

const MatchedCompanies = ({data}) => {
    if(data == null){
        return '';
    }
    
    const {
        nCompanies,
        nMatches,
        nUniqueMatches,
        nDuplicates,
        percentMatches,
        percentUniqueMatches,
        percentDuplicates
    } = data;
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

const SectorMatches = ({data}) => {

    if(data == null){
        return '';
    }
    
    const {
        nCompanies,
        nSectors,
        nMatches
    } = data;
    
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
                            <td>{sector.name}</td>
                            <td>{sector.count}</td>
                          </tr>
                      );
                  })
              }
            </tbody>
          </table>
        </div>        
    );
};

const CompanyOrderSummary = ({data}) => {
    const [nCompanies, nCompaniesWithOrders] = data.data;
    return (
        <div className="company-order-summary">
          <h3>Company order summary</h3>
          <table className="table table-striped">
            <tbody>
            <tr><td>#Companies</td><td>{nCompanies}</td></tr>
            <tr><td>#Companies with orders</td><td>{nCompaniesWithOrders}</td></tr>
              <tr>
                <td>%Companies with orders</td>
                <td>{Math.round(100 * 100 * nCompaniesWithOrders/nCompanies)/100}</td>
              </tr>
            </tbody>
          </table>
        </div>
    );

};

const CompanyExportCountriesSummary = ({data}) => {
    if(data == null){
        return '';
    }
    
    const [nCompanies, nWithExportCountries] = data.data;
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

const CompanyCountriesOfInterestSummary = ({data}) => {
    if(data == null){
        return '';
    }

    const [nCompanies, nCompaniesWithCountriesOfInterest] = data.data;
    return (
        <div className="company-countries-of-interest-summary">
          <h3>Countries of interest summary</h3>
          <table className="table table-striped">
            <tbody>
              <tr><td>#Companies</td><td>{nCompanies}</td></tr>
              <tr>
                <td>#Companies with countries of interest</td>
                <td>{nCompaniesWithCountriesOfInterest}</td>
              </tr>
              <tr>
                <td>%Companies with countries of interest</td>
                <td>{Math.round(100 * 100 * nCompaniesWithCountriesOfInterest/nCompanies)/100}</td>
              </tr>
            </tbody>
          </table>
        </div>
    );
};

const OrderFrequency = ({data, frequency, onChange, chartId}) => {
    if(data == null){
        return '';
    }

    const onChange2 = (e) => {
        return onChange(frequencyInput.value);
    };

    let frequencyInput
    const freqData = {...data[frequency]};
    
    return (
        <div className='order-frequency'>
          <h3>Order frequency</h3>
          <label className="form-label">Frequency</label>
          <select className="form-control" onChange={onChange2} ref={input=>frequencyInput=input}>
            <option value="daily">Daily</option>
            <option value="weekly">Weekly</option>
            <option value="monthly">Monthly</option>
          </select>
          <OrderFrequencyChart data={freqData} id={chartId}/>
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
