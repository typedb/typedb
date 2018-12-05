var env_preset = require('@babel/preset-env');
var minify = require('babel-preset-minify');

const babelConfig = {
    presets: [
        [env_preset, {targets: {node: "6.5.0"}}],
        minify],
};

module.exports = babelConfig;
