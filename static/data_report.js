import React from 'react';
import axios from 'axios';
import { render } from 'react-dom';

window.React = React;

class App extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            data: {
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
                companyOrderSummary: {
                    nCompanies: 10,
                    nCompaniesWithOrders: 2
                },
                companyExportCountriesSummary: {
                    nCompanies: 10,
                    nWithExportCountries: 1
                }
            },
            nRowsTopSector: 5
        };
        this.onNRowsTopSectorChange = this.onNRowsTopSectorChange.bind(this);
    }

    componentDidMount() {
        console.log("component mounted");
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
    }

    onNRowsTopSectorChange(nRows){
        console.log('go');
        console.log(nRows);
        this.setState({nRowsTopSector: nRows});
    }
    
    render() {

        const {data, nRowsTopSector} = this.state;
        return (
            <div>
              <MatchedCompanies data={data.matchedCompanies}/>
              <SectorMatches data={data.sectorMatches}/>
              <TopSectors
                data={data.topSectors}
                onChange={this.onNRowsTopSectorChange}
                nRows={nRowsTopSector}
              />
              <CompanyOrderSummary data={data.companyOrderSummary}/>
              <CompanyExportCountriesSummary data={data.companyExportCountriesSummary}/>
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
    const {nCompanies, nCompaniesWithOrders} = data;
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
    
    const {nCompanies, nWithExportCountries} = data;
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

render(<App/>, document.getElementById('react-container'));
