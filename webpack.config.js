const path = require('path');

module.exports = {
    devtool: 'source-map',
    entry: {
	'vendors': './app/static/vendors.js',
	'data_report': './app/static/data_report.js',
    },
    mode: 'development',
    output: {
	path: path.resolve(__dirname, 'app/static/dist'),
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
