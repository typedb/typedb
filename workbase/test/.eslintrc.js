module.exports = {
  env: {
    "jest/globals": true,
    "jest": true
  },
  plugins: ["jest"],
  rules: {
    "jest/no-disabled-tests": "warn",
    "jest/no-focused-tests": "error",
    "jest/no-identical-title": "error",
    "no-extend-native": ["error", { "exceptions": ["Array"] }]
  },
  extends: ["plugin:jest/recommended"]
}
