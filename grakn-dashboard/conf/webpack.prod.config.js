const merge = require('webpack-merge');
const baseWpConfig = require('./webpack.base.config');
const webpack = require('webpack');

module.exports = merge(baseWpConfig, {
  module: {
    rules: [
     {
        test: /\.vue$/,
        loader: 'vue-loader',
      },
    ],
  },
  plugins: [
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify('production'),
      },
    }),
    new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false,
      },
    }),
    new webpack.LoaderOptionsPlugin({
      minimize: true,
    }),
  ],
});
