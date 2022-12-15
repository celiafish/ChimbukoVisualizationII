import React from 'react';
import PropTypes from 'prop-types';

import { Line } from 'react-chartjs-2';

class AnomalyHistory extends React.Component
{
    constructor(props) {
        super(props);
        this.state = {
            data: [],
        };
        this.socketio = props.socketio;
        this.pause = false;
    }

    componentDidMount() {
        this.register_io();
    }

    register_io = () => {
        if (this.socketio) {
            this.socketio.on('update_history', data => {
                this.updateChartData(data);
            });
        }
    }

    updateChartData = chartData => {
        if (chartData.length === 0)
            return;

        // console.log(chartData);
        if (this.pause)
            return;

        const { data:newData } = chartData;
        let { data:dataState } = this.state;

        if (dataState.length == 0) {
            newData.forEach( (category, i) => {
                const color = [{r: 0, g: 204, b: 255}, {r: 255, g: 150, b: 150}];
                dataState.push({
                    'color': color[i],
                    'name': category.name,
                    'stat': []
                }); 
            });
        }

        newData.forEach((category, index) => {
            dataState[index].stat = category.stat;
        });

        this.setState({...this.state, data: dataState});
    }

    shouldComponentUpdate(nextProps, nextState) {
        console.log("test component update");

        const { pause } = nextProps;
        this.pause = pause
        if (this.chart) {
            if (pause)
                return false;
            else
                return true;
        }
        return false;
    }

    handleSwitch = name => ev => {
        this.setState({...this.state, [name]: ev.target.checked});
    }

    handleBarClick = elem => {
        if (elem.length == 0)
            return;

        const datasetIndex = elem[0]._datasetIndex;
        const index = elem[0]._index;
        let stat = this.state.data[datasetIndex].stat[index];

        if (this.props.onBarClick)
            this.props.onBarClick(stat);
    }

    render() {
        const { height } = this.props;
        const { data } = this.state;

        const info = [];
        const lineData = [];
        let maxLen = 0;
        data.forEach((category, index) => {
            const rgb = category.color;
            if (index == 0) { // ranks
                lineData.push({
                    label: category.name,
                    data: category.stat.map(d => d[2]),
                    backgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.2)`,
                    borderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,
                    borderWidth: 1,
                    hoverBackgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.4)`,
                    hoverBorderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,        
                });
                info.push(category.stat.map(d => `${d[0]}:${d[1]}`));
            }
            else { // funcitons
                lineData.push({
                    label: category.name,
                    data: category.stat.map(d => d[3]),
                    backgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.2)`,
                    borderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,
                    borderWidth: 1,
                    hoverBackgroundColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.4)`,
                    hoverBorderColor: `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 1)`,        
                });
                info.push(category.stat.map(d => `${d[0]}:${d[1]}:${d[2]}`));
            }

            if (category.stat.length > maxLen)
                maxLen = category.stat.length;
        });

        const _data = {
            labels: maxLen==0?[]:Array(maxLen).fill(0).map((_, i) => i),  //info[0], 
            datasets: lineData
        };

        return (
            <Line 
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
                                if (datasetIndex == 0) {    
                                    return `App:Rank-${info[datasetIndex][index]}`;
                                }
                                else {
                                    return `App:Fid:Name-${info[datasetIndex][index]}`;
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

AnomalyHistory.defaultProps = {
    height: 100,
    onBarClick: (_) => {},
};

AnomalyHistory.propTypes = {
    width: PropTypes.number,
    height: PropTypes.number.isRequired,
    onBarClick: PropTypes.func
};

export default AnomalyHistory;
