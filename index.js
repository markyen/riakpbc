var Stream = require('stream');
var inspect = require('eyespect').inspector();
var net = require('net'),
protobuf = require('protobuf.js'),
butils = require('butils'),
util = require('util'),
through = require('through'),
path = require('path'),
merge = require('./lib/merge'),
setImmediate = setImmediate || process.nextTick;


var messageCodes = {
  '0': 'RpbErrorResp',
  '1': 'RpbPingReq',
  '2': 'RpbPingResp',
  '3': 'RpbGetClientIdReq',
  '4': 'RpbGetClientIdResp',
  '5': 'RpbSetClientIdReq',
  '6': 'RpbSetClientIdResp',
  '7': 'RpbGetServerInfoReq',
  '8': 'RpbGetServerInfoResp',
  '9': 'RpbGetReq',
  '10': 'RpbGetResp',
  '11': 'RpbPutReq',
  '12': 'RpbPutResp',
  '13': 'RpbDelReq',
  '14': 'RpbDelResp',
  '15': 'RpbListBucketsReq',
  '16': 'RpbListBucketsResp',
  '17': 'RpbListKeysReq',
  '18': 'RpbListKeysResp',
  '19': 'RpbGetBucketReq',
  '20': 'RpbGetBucketResp',
  '21': 'RpbSetBucketReq',
  '22': 'RpbSetBucketResp',
  '23': 'RpbMapRedReq',
  '24': 'RpbMapRedResp',
  '25': 'RpbIndexReq',
  '26': 'RpbIndexResp',
  '27': 'RpbSearchQueryReq',
  '28': 'RpbSearchQueryResp',
  // 1.4
  '29': 'RpbResetBucketReq',
  '30': 'RpbResetBucketResp',
  '40': 'RpbCSBucketReq',
  '41': 'RpbCSBucketResp',
  '50': 'RpbCounterUpdateReq',
  '51': 'RpbCounterUpdateResp',
  '52': 'RpbCounterGetReq',
  '53': 'RpbCounterGetResp',
};
Object.keys(messageCodes).forEach(function(key) {
  messageCodes[messageCodes[key]] = Number(key);
});

function RiakPBC(options) {
  var self = this;
  options = options || {};
  self.host = options.host || '127.0.0.1';
  self.port = options.port || 8087;
  self.timeout = options.timeout || 1000;
  self.bucket = options.bucket || undefined;
  self.translator = protobuf.loadSchema(path.join(__dirname, './spec/riak_kv.proto'));
  self.client = new net.Socket();
  self.connected = false;
  self.client.on('end', self.disconnect);
  self.client.on('error', self.disconnect);
  self.paused = false;
  self.queue = [];
  var mc, reply = {}, resBuffers = [],
  numBytesAwaiting = 0;

  function splitPacket(pkt) {
    var pos = 0,
    len;
    if (numBytesAwaiting > 0) {
      len = Math.min(pkt.length, numBytesAwaiting);
      var oldBuf = resBuffers[resBuffers.length - 1];
      var newBuf = new Buffer(oldBuf.length + len);
      oldBuf.copy(newBuf, 0);
      pkt.slice(0, len).copy(newBuf, oldBuf.length);
      resBuffers[resBuffers.length - 1] = newBuf;
      pos = len;
      numBytesAwaiting -= len;
    }
    else {
      resBuffers = [];
    }
    while (pos < pkt.length) {
      len = butils.readInt32(pkt, pos);
      numBytesAwaiting = len + 4 - pkt.length;
      resBuffers.push(pkt.slice(pos + 4, Math.min(pos + len + 4, pkt.length)));
      pos += len + 4;
    }
  }

  self.client.on('data', function(chunk) {
    splitPacket(chunk);
    if (numBytesAwaiting > 0) {
      return;
    }
    processAllResBufers();
  });

  function processAllResBufers() {
    resBuffers.forEach(processSingleResBuffer);
  }

  function handleResponseError(response) {
    var err;
    var stream = self.task.stream;
    var callback = self.task.callback;
    if (response.errmsg) {
      err = new Error(response.errmsg);
      err.code = response.errcode;
    }
    if (!err) {
      return;
    }
    if (stream) {
      stream.emit('error', err);
    }
    if (callback) {
      callback(err);
    }
    return err;
  }

  function isLastResponse(reply) {
    if (!self.task.expectMultiple) {
      return true;
    }
    if (reply.done) {
      return true;
    }
    if (mc === 'RpbErrorResp') {
      return true;
    }
  }

  function processSingleResBuffer(packet) {
    var err;
    var stream = self.task.stream;
    mc = messageCodes['' + packet[0]];

    var response = self.translator.decode(mc, packet.slice(1));
    if (response.content && Array.isArray(response.content)) {
      response.content.forEach(parseItemContent);
    }
    err = handleResponseError(response);
    if (err) {
      return;
    }
    if (stream && !response.done) {
      self.task.stream.write(response);
    }
    if (stream) {
      reply = response;
    }
    else {
      reply = merge(reply, response);
    }
    if (isLastResponse(reply)) {
      outputResponse(reply);
      self.task = undefined;
      mc = undefined;
      reply = {};
      self.paused = false;
      err = undefined;
      setImmediate(self.processNext);
    }
  }

  function outputResponse(reponse) {
    var stream = self.task.stream;
    var cb = self.task.callback;
    if (stream) {
      stream.end();
    }
    else {
      cb(null, reply);
    }
  }

  self.processNext = function() {
    if (self.queue.length && !self.paused) {
      self.paused = true;
      self.connect(function(err) {
        self.task = self.queue.shift();
        if (err) return self.task.callback(err);
        self.client.write(self.task.message);
      });
    }
  };
}

RiakPBC.prototype.makeRequest = function(type, data, callback, expectMultiple, stream) {
  var self = this,
  reply = {},
  buffer = this.translator.encode(type, data),
  message = [];

  butils.writeInt32(message, buffer.length + 1);
  butils.writeInt(message, messageCodes[type], 4);
  message = message.concat(buffer);
  self.queue.push({
    message: new Buffer(message),
    callback: callback,
    expectMultiple: expectMultiple,
    stream: stream
  });
  setImmediate(self.processNext);
};

RiakPBC.prototype.getBuckets = function(callback) {
  this.makeRequest('RpbListBucketsReq', null, callback);
};

RiakPBC.prototype.getBucket = function(params, callback) {
  this.makeRequest('RpbGetBucketReq', params, callback);
};

RiakPBC.prototype.setBucket = function(params, callback) {
  this.makeRequest('RpbSetBucketReq', params, callback);
};

RiakPBC.prototype.resetBucket = function(params, callback) {
  this.makeRequest('RpbResetBucketReq', params, callback);
};

RiakPBC.prototype.getKeys = function(params, streaming, callback) {
  if (typeof streaming === 'function') {
    callback = streaming;
    streaming = false;
  }

  if (streaming) {
    var stream = writableStream();
    this.makeRequest('RpbListKeysReq', params, callback, true, stream);
    return stream;
  }
  else {
    this.makeRequest('RpbListKeysReq', params, callback, true);
  }
};

RiakPBC.prototype.put = function(params, callback) {
  this.makeRequest('RpbPutReq', params, callback);
};

RiakPBC.prototype.get = function(params, callback) {
  this.makeRequest('RpbGetReq', params, callback);
};

RiakPBC.prototype.del = function(params, callback) {
  this.makeRequest('RpbDelReq', params, callback);
};

RiakPBC.prototype.mapred = function(params, streaming, callback) {
  if (typeof streaming === 'function') {
    callback = streaming;
    streaming = false;
  }

  if (streaming) {
    var stream = writableStream();
    this.makeRequest('RpbMapRedReq', params, callback, true, stream);
    return parseMapReduceStream(stream);
  }
  else {
    this.makeRequest('RpbMapRedReq', params, callback, true);
  }
};


RiakPBC.prototype.getCounter = function(params, callback) {
  this.makeRequest('RpbCounterGetReq', params, callback);
};


RiakPBC.prototype.updateCounter = function(params, callback) {
  this.makeRequest('RpbCounterUpdateReq', params, callback);
};

RiakPBC.prototype.getIndex = function(params, callback, streaming) {
  if (typeof streaming === 'function') {
    callback = streaming;
    streaming = false;
  }

  if (streaming) {
    var stream = writableStream();
    this.makeRequest('RpbIndexReq', params, callback, true, stream);
    return stream;
  }
  else {
    this.makeRequest('RpbIndexReq', params, callback);
  }
};

RiakPBC.prototype.search = function(params, callback) {
  this.makeRequest('RpbSearchQueryReq', params, callback);
};

RiakPBC.prototype.getClientId = function(callback) {
  this.makeRequest('RpbGetClientIdReq', null, callback);
};

RiakPBC.prototype.setClientId = function(params, callback) {
  this.makeRequest('RpbSetClientIdReq', params, callback);
};

RiakPBC.prototype.getServerInfo = function(callback) {
  this.makeRequest('RpbGetServerInfoReq', null, callback);
};

RiakPBC.prototype.ping = function(callback) {
  this.makeRequest('RpbPingReq', null, callback);
};

RiakPBC.prototype.connect = function(callback) {
  if (this.connected) return callback(null);
  var self = this;

  var timeoutGuard = setTimeout(function() {
    callback(new Error('Connection timeout'));
  }, self.timeout);

  self.client.connect(self.port, self.host, function() {
    clearTimeout(timeoutGuard);
    self.connected = true;
    callback(null);
  });
};

RiakPBC.prototype.disconnect = function () {
  if (!this.connected) return;
  this.client.end();
  this.connected = false;
  if (this.task) {
    this.queue.unshift(this.task);
    this.task = undefined;
  }
};

exports.createClient = function (options) {
  return new RiakPBC(options);
};

function writableStream() {
  var ts = through(function (data) {
    this.queue(data);
  });
  return ts;
}

function parseMapReduceStream(rawStream) {
  var liner = new Stream.Transform({
    objectMode: true
  });

  liner._transform = function(chunk, encoding, done) {
    var response = chunk.response;
    var json = JSON.parse(response);
    var self = this;
    json.forEach(function(row) {
      self.push(row);
    });
    done();
  };

  rawStream.on('error', function(err) {
    liner.emit('error', err);
  });
  rawStream.pipe(liner);
  return liner;
}

function parseItemContent(item) {
  if (!item.value || !item.content_type) {
    return;
  }

  if (item.content_type.match(/^(text\/\*)|(application\/json)$/)) {
    item.value = item.value.toString();
  }
  if (item.content_type === 'application/json') {
    item.value = JSON.parse(item.value.toString());
  }
}
