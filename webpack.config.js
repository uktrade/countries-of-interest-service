const path = require('path');

module.exports = {
    devtool: 'source-map',
    entry: {
	'vendors': './static/vendors.js',
	'data_report': './static/data_report.js',
    },
    mode: 'development',
    output: {
	path: path.resolve(__dirname, 'static/dist'),
	filename: '[name].bundle.js'
    },
    module: {
	rules: [
	    {
		test: /\.js$/,
		exclude: /(node_modules)/,
		loader: 'babel-loader',
		query: {
		    presets: ['@babel/preset-env', '@babel/preset-react']
		}
	    },
	    {
		test: /\.css$/,
		use: ['style-loader', 'css-loader']
	    }
	],
	
    }
}
