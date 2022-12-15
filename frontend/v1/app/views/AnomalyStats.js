import React from 'react';
import PropTypes from 'prop-types';

import { Bar } from 'react-chartjs-2';

class AnomalyStats extends React.Component 
{
    constructor(props) {
        super(props);
        this.state = {
            data: [],
            colors: {}
        };

        this.socketio = props.socketio;
    }

    componentDidMount() {
        this.register_io();
    }

    register_io = () => {
        if (this.socketio) {
            this.socketio.on('update_stats', data => {
                this.updateChartData(data);
            });            
        }
    }

    updateChartData = chartData => {
        const { data:newData } = chartData;
        const { nQueries, statKind } = this.props;
        let { colors, data:dataState } = this.state;

        if (dataState.length === 0) {
            newData.forEach((category, i) => {
                const color = [{r: 0, g: 204, b: 255}, {r: 255, g: 150, b: 150}];
                dataState.push({
                    'color': color[i],
                    'name': category.name,
                    'stat': []
                });      
            });
        }

        //if (newData.length == 2)
        //    console.log(newData[1]);

        newData.forEach( (category, index) => {
            /*
            ////// for anomaly_stats data //////
            ////// need to filter with ts //////
            if (index == 0) {
                const keys = new Set([]);
                let stat = [...dataState[index].stat, ...category.stat];
                stat.sort((a, b) => b.created_at - a.created_at);
                stat = stat.filter((d, i) => {
                        if (keys.has(d.key)) 
                            return false;
                        keys.add(d.key);
                        return true;
                    }); // when merged, only keep latest
                stat.sort((a, b) => b[statKind] - a[statKind]);
                if (nQueries < stat.length)
                    stat = stat.slice(0, nQueries);
                dataState[index].stat = stat;
            }
            else {
                let stat = category.stat;
                stat.sort((a, b) => b.stats.mean - a.stats.mean); // only consider mean for now
                if (nQueries < stat.length)
                    stat = stat.slice(0, nQueries);
                dataState[index].stat = stat;
            }
            */
            ////// for anomaly_metrics //////
            ////// partial order: (count, ts) //////
            let stat = [...dataState[index].stat, ...category.stat];
            stat.sort((a, b) => {
                if (a.count == b.count) {
                    return a.created_at > b.created_at ? -1 : 1;
                }
                else {
                    return a.count > b.count ? -1 : 1;
                }
            })
            const keys = new Set([]);
            stat = stat.filter((d, i) => {
                if (keys.has(d.key))
                    return false;
                keys.add(d.key);
                return true;
            }); // when merged, only keep latest
            if (nQueries < stat.length)
                stat = stat.slice(0, nQueries);
            dataState[index].stat = stat;
        });

        // console.log(dataState);

        this.setState({...this.state, data: dataState, colors});
    }
                
    handleBarClick = elem => {
        // first entry of bar click
        
        if (elem.length == 0)
            return;

        const datasetIndex = elem[0]._datasetIndex;
        const index = elem[0]._index;
        let stat = this.state.data[datasetIndex].stat[index];

        if (this.props.onBarClick)
            this.props.onBarClick(stat);
    }

    render() {
        const {height, statKind} = this.props;
        const { data } = this.state;

        const ranks = [];
        const ioSteps = []; 
        const barData = [];
        let maxLen = 0;
        data.forEach((category, index) => {
            const rgb = category.color;
            if (index == 0) {
                barData.push({
                    label: category.name,
                    data: category.stat.map(d => d.count), // d => d[statKind]
                    backgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.2)`,
                    borderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,
                    borderWidth: 1,
                    hoverBackgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.4)`,
                    hoverBorderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,        
                });
                ranks.push(category.stat.map(d => d.key));
                ioSteps.push(category.stat.map(d => `${d.first_io_step}-${d.last_io_step}`));
            }
            else {
                barData.push({
                    label: category.name,
                    data: category.stat.map(d => d.count), // d => d.stats[statKind]
                    backgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.2)`,
                    borderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,
                    borderWidth: 1,
                    hoverBackgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.4)`,
                    hoverBorderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,        
                });
                ranks.push(category.stat.map(d => `${d.key}:${d.name}`));
                ioSteps.push(category.stat.map(d => `${d.first_io_step}-${d.last_io_step}`));
            }

            if (category.stat.length > maxLen)
                maxLen = category.stat.length;
        });

        const _data = {
            labels: maxLen==0?[]:Array(maxLen).fill(0).map((_, i) => i), //ranks[0], // 
            datasets: barData
        };
        // console.log("ready to show AnomalyStats:");
        // console.log(ranks);
        // console.log(_data);

        return (
            <Bar 
                ref={ref => this.chart = ref}
                data={_data}
                height={height}
                getElementAtEvent={this.handleBarClick}
                options={{
                    maintainAspectRatio: false,
                    tooltips: {
                        callbacks: {
                            title: (tooltipItem, data) => {
                                const datasetIndex = tooltipItem[0].datasetIndex;
                                const index = tooltipItem[0].index;
                                const content1 = ranks[datasetIndex][index];
                                const content2 = ioSteps[datasetIndex][index];
                                if (datasetIndex == 0) {    
                                    return `App:Rank-${content1}; Step:${content2}`;
                                }
                                else {
                                    return `App:Fid:Name-${content1}; Step:${content2}`;
                                }
                            }
                        }
                    },
                    scales: {
                        yAxes: [{
                            scaleLabel: {
                                display: true,
                                labelString: `severity` //statKind
                            }
                        }]
                    }         
                }}
            />
        );
    }
}

AnomalyStats.defaultProps = {
    height: 100,
    stats: {},
    onBarClick: (_) => {}, // as given in appindex
    nQueries: 5,
    statKind: 'stddev'
};

AnomalyStats.propTypes = {
    width: PropTypes.number,
    height: PropTypes.number.isRequired,
    stats: PropTypes.object,
    onBarClick: PropTypes.func
};

export default AnomalyStats;