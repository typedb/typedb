import CodeMirror from 'codemirror';

CodeMirror.defineSimpleMode('graql', {
  // The start state contains the rules that are intially used
  start: [
    { regex: /#.*/, token: 'comment' },
    { regex: /".*?"/, token: 'string' },
    { regex: /(match|ask|insert|delete|select|isa|sub|plays|relates|has-scope|datatype|is-abstract|has|value|id|of|limit|offset|order|by|compute|from|to|in|aggregate|label)(?![-a-zA-Z_0-9])/, // eslint-disable-line max-len
      token: 'keyword' },
    { regex: /true|false/, token: 'number' },
    { regex: /\$[-a-zA-Z_0-9]+/, token: 'variable' },
    { regex: /[-a-zA-Z_][-a-zA-Z_0-9]*/, token: 'identifier' },
    { regex: /[0-9]+(\.[0-9][0-9]*)?/, token: 'number' },
    { regex: /=|!=|>|<|>=|<=|contains|regex/, token: 'operator' },
  ],
  comment: [],
  // The meta property contains global information about the mode. It
  // can contain properties like lineComment, which are supported by
  // all modes, and also directives like dontIndentStates, which are
  // specific to simple modes.
  meta: {
    dontIndentStates: ['comment'],
    lineComment: '#',
  },
});
