/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Wrapper for Duplex Stream that exposes method to send new requests and returns
 * responses as results of Promises.
 * @param {*} stream 
 */
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
  if(!this.stream.writable) throw "Transaction is already closed.";
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