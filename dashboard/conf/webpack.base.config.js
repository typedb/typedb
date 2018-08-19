const path = require('path');

module.exports = {
  entry: './src/main.js',
  target: 'web',
  output: {
    filename: '../static/dashboard.js',
    path: path.resolve(__dirname, '../dist'),
  },
  module: {
    rules: [
      {
        test: /\.js$/,
        loader: 'babel-loader',
        exclude: [/node_modules/, /vendor/],
      },
      {
        test: /\.vue$/,
        loader: 'vue-loader',
        options: {
          esModule: false,
        }
      },
    ],
  },
  resolve: {
    alias: {
      vue$: 'vue/dist/vue.esm.js',
    }
  },
  plugins: [
    function timestamp() {
      this.plugin('watch-run', (watching, callback) => {
        console.log(`Begin compile at ${new Date()}`);
        callback();
      });
    }
  ],
};
