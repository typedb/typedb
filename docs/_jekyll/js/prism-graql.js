/**
 * Contains the Graql syntax highlighter for Prism.
 * Written by Felix
 * Modified by Harjyot
 */

(function () {
  Prism.languages.graql = {
    comment: {
        pattern: /#.*/,
        alias: 'comment'
    },
    string: {
        pattern: /".*?"/,
        alias: 'string'
    },
    keyword: {
        pattern: /((?:(?![-a-zA-Z_0-9]|\$).)|^|\s)(and|concat|do|boolean|double|long|string|date|group|label|graql|commit|match|select|get|define|insert|delete|where|isa|sub|plays|relates|key|has|has-scope|datatype|is-abstract|has|value|id|of|limit|offset|order|by|compute|in|std|median|mean|max|min|sum|count|degrees|path|cluster|lhs|rhs|when|then|if|else|elseif|for|aggregate|asc|desc|from|to|val|contains|regex|distinct|not|noescp|null)(?![-a-zA-Z_0-9])/,
        alias: 'keyword',
        lookbehind: true
    },
    super: {
        pattern: /((?:(?![-a-zA-Z_0-9]|\$).)|^|\s)(entity|role|relationship|attribute|rule|concept|thing)(?![-a-zA-Z_0-9])/,
        alias: 'system',
        lookbehind: true
    },
    special: {
        pattern: /graql>|results>|\.\.\./
    },
    variable: {
        pattern: /\$[-a-zA-Z_0-9]+/,
        alias: 'variable'
    },
    type: {
        pattern: /[-a-zA-Z_][-a-zA-Z_0-9]*/,
        alias: 'function'
    },
    number: {
        pattern: /[0-9]+(\.[0-9][0-9]*)?/,
        alias: 'number'
    },
    operator: {
        pattern: /=|;|\.|\+|\-|\*|,|\(|\)|:|{|}|!=|>|<|>=|<=/,
        alias: 'operator'
    }
  };
}());