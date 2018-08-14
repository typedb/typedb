module.exports = {
  root: true,
  parser: 'babel-eslint',
  parserOptions: {
    sourceType: 'module'
  },
  plugins: [
    "import",
    "html",
    "vue"  
  ],
  env: {
    browser: true,
    node: true,
  },
  extends: 'airbnb-base',
  globals: {
    __static: true
  },
  rules: {
    'global-require': 0,
    'import/no-unresolved': 0,
    'no-param-reassign': 0,
    'no-shadow': 0,
    'import/extensions': 0,
    'import/newline-after-import': 0,
    'no-multi-assign': 0,
    // allow debugger during development
    'no-debugger': process.env.NODE_ENV === 'production' ? 2 : 0,
    'max-len': ['error', 180 , {'ignoreComments': true}],
    "no-underscore-dangle": ["error", { "allowAfterThis": true }],
    "no-extend-native": ["error", { "exceptions": ["Array"] }]
  },
  settings:{
    'import/core-modules': ['electron']
  }
}
