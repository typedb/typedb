
function GrpcCommunicator(stream) {
  this.stream = stream;
  this.pending = [];

  this.stream.on("data", resp => {
    this.pending.shift().resolve(resp);
  });

  this.stream.on("error", err => {
    this.pending.shift().reject(err);
  });

  this.stream.on('status', (e) => {
    if (this.pending.length) this.pending.shift().reject(e);
  })
}

GrpcCommunicator.prototype.send = async function (request) {
  return new Promise((resolve, reject) => {
    this.pending.push({ resolve, reject });
    this.stream.write(request);
  })
};

GrpcCommunicator.prototype.end = function end() {
  this.stream.end();
  return new Promise((resolve) => {
    this.stream.on('end', resolve);
  });
}

module.exports = GrpcCommunicator;