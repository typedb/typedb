[![Build Status](https://travis-ci.org/jfromaniello/npm-install-retry.svg?branch=master)](https://travis-ci.org/jfromaniello/npm-install-retry)

![Build status](https://ci.appveyor.com/api/projects/status/sc7937we6gb0mwoc?svg=true)

Command line utility that retries  `npm install` when NPM fails with `npm ERR! cb() never called` or `npm ERR! errno ECONNRESET`.

This happens sporadically and has been reported many times:

-  https://github.com/meteor/meteor/issues/1190
-  https://github.com/isaacs/npm/issues/2907
-  https://github.com/isaacs/npm/issues/3269
-  https://github.com/npm/npm/issues?utf8=%E2%9C%93&q=ECONNRESET+

and still fails.


## Installation

	npm install -g  npm-install-retry

## Usage

From command-line:

	npm-install-retry --wait 500 --attempts 10 -- --production

It has two options wait (defaults to 500) and attempts (default to 10). Everything after `--` goes directly to npm.

## License

MIT 2013 - José F. Romaniello
