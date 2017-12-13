const path = require('path');
const express = require('express');

const app = express();

const port = process.env.PORT ? process.env.PORT : 3000;
const dist = path.join(__dirname, '_site');

app.get('*', (req, res) => {
  let requestedResource = req.path;
  if (requestedResource.indexOf('.') === -1 && requestedResource !== '/') {
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