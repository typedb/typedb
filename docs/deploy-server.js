const path = require('path');
const express = require('express');

const app = express();

const port = process.env.PORT ? process.env.PORT : 3000;
const dist = path.join(__dirname, '_site');

app.use('/images', express.static(path.join(__dirname, 'images')));


app.get('*', (req, res) => {
  let requestedResource = req.path;
  if (requestedResource.match(/^\/(index)?(.html)?$/)) {
    requestedResource = '/docs';
    res.redirect(301,`${req.protocol}://${req.get('host')}${requestedResource}`);
  }
  else if (requestedResource.match(/^\/documentation/)) {
    requestedResource = req.path.replace('/documentation', '/docs');
    res.redirect(302,`${req.protocol}://${req.get('host')}${requestedResource}`);
  }
  if(requestedResource.match(/^\/(docs|overview|academy|contributors)\/?$/)){
    const indexlink = requestedResource[requestedResource.length - 1] === '/'? 'index.html' : '/index.html';
    requestedResource = requestedResource.concat(indexlink);
  }
  else if (!requestedResource.match(/\.(svg|pdf|png|jpg|jpeg|ico|ttf|otf|woff|woff2|eot|css|js|html|json|xml)$/) && requestedResource !== '/') {
    requestedResource = requestedResource.concat('.html');
  }
  res.sendFile(path.join(dist, requestedResource), (error) => {
    if(error && error.statusCode === 404 && error.path.match(/\.html$/)) {
      res.sendFile(path.join(dist, '/404.html'));
    }
  });
});


app.listen(port, (error) => {
  if (error) {
    console.log(error); // eslint-disable-line no-console
  }
  console.info('Express is listening on port %s.', port); // eslint-disable-line no-console
});
