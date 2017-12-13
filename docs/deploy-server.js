const path = require('path');
const express = require('express');

const app = express();

const port = process.env.PORT ? process.env.PORT : 3000;
const dist = path.join(__dirname, '_site');

app.use('/images', express.static(path.join(__dirname, 'images')));


app.get('*', (req, res) => {
  let requestedResource = req.path;
  if (requestedResource.match(/^\/documentation/)) {
    requestedResource = req.path.replace('/documentation', '/docs');
  }
  if(requestedResource.match(/^\/(overview|academy|contributors)\/?$/)){
    const indexlink = requestedResource[requestedResource.length - 1] === '/'? 'index.html' : '/index.html';
    requestedResource = requestedResource.concat(indexlink);
  }
  else if (!requestedResource.match(/\.(svg|pdf|png|jpg|jpeg|ico|ttf|otf|woff|woff2|eot|css|js|html|json|xml)$/) && requestedResource !== '/') {
    requestedResource = requestedResource.concat('.html');
  }
  res.sendFile(path.join(dist, requestedResource));
});


app.listen(port, (error) => {
  if (error) {
    console.log(error); // eslint-disable-line no-console
  }
  console.info('Express is listening on port %s.', port); // eslint-disable-line no-console
});