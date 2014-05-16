var Stream = require('stream');
var Protobuf = require('protobuf.js');
var riakproto = require('riakproto');
var _merge = require('./lib/merge');
var quorum = require('./lib/quorum');
var parseResponse = require('./lib/parse-response');
var ConnectionManager = require('./lib/connection-manager');

function RiakPBC(options) {
    options = options || {};
    options.host = options.host || '127.0.0.1';
    options.port = options.port || 8087;
    options.timeout = options.timeout || 1000;
    options.auto_connect = options.hasOwnProperty('auto_connect') ? options.auto_connect : true;

    this.connection = new ConnectionManager(options);
    this.connection.receive = this._processMessage.bind(this);
    this.connection.riak = this;

    this.translator = new Protobuf(riakproto);

    this.queue = [];
    this.reply = {};
}

RiakPBC.prototype._processMessage = function (data) {
    var response, messageCode, err, done;

    messageCode = riakproto.codes['' + data[0]];
    response = this.translator.decode(messageCode, data.slice(1));

    if (!response) {
        err = new Error('Failed to decode response message');

        return this._cleanup(err);
    }

    response = parseResponse(response);

    if (response.errmsg) {
        err = new Error(response.errmsg);
        err.code = response.errcode;

        return this._cleanup(err);
    }

    if (response.done) {
        done = true;
        delete response.done;
    }

    if (this.task.callback) {
        this.reply = _merge(this.reply, response);
    } else if (Object.keys(response).length) {
        this.task.stream.write(response);
    }

    if (done || !this.task.expectMultiple || messageCode === 'RpbErrorResp') {
        this._cleanup(undefined, this.reply);
    }
};

RiakPBC.prototype._cleanup = function (err, reply) {
    this.reply = {};

    if (this.task.callback) {
        this.task.callback(err, reply);
    } else {
        if (err) {
            this.task.stream.emit('error', err);
        } else {
            this.task.stream.end();
        }
    }

    this.task = undefined;
    this._processNext();
};

RiakPBC.prototype._processNext = function () {
    var self = this;

    if ((self.connection.connecting && !self.connection.queue.length) ||
        (!self.connection.queue.length && !self.queue.length) ||
        self.task) {
        return;
    }

    var _error = function (err) {
        if (self.task.callback) {
            return self.task.callback(err);
        }

        self.task.stream.emit('error', err);
    };

    if (!self.connection.connected && self.connection.auto_connect) {
        return self.connection.connect(function (err) {
            if (err && self.queue.length) {
                self.task = self.queue.shift();
                _error(err);
            }
            self._processNext();
        });
    }

    self.task = self.connection.queue.length ? self.connection.queue.shift() : self.queue.shift();

    self.connection.send(self.task.message, function (err) {
        if (err) {
            _error(err);
        }
    });
};

RiakPBC.prototype._makeTask = function (opts) {
    var buffer, message, stream = null, cb = null;

    if (riakproto.messages[opts.type]) {
        buffer = this.translator.encode(opts.type, opts.params);
    } else {
        buffer = new Buffer(0);
    }

    message = new Buffer(buffer.length + 5);

    if (typeof opts.callback === 'function') {
        var _cb = opts.callback;
        cb = function (_err, _reply) {
            process.nextTick(function () {
                _cb(_err, _reply);
            });
        };
    } else {
        stream = writableStream();
    }

    message.writeInt32BE(buffer.length + 1, 0);
    message.writeUInt8(riakproto.codes[opts.type], 4);
    buffer.copy(message, 5);

    return {
        message: message,
        callback: cb,
        expectMultiple: opts.expectMultiple,
        stream: stream
    };
};

// RiakPBC.prototype.makeRequest = function (type, data, callback, expectMultiple, streaming) {
RiakPBC.prototype.makeRequest = function (opts) {
    var task = this._makeTask(opts);

    this.queue.push(task);
    this._processNext();

    return task.stream;
};

RiakPBC.prototype.getBuckets = function (params, callback) {
    return this.makeRequest({
        type: 'RpbListBucketsReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.getBucket = function (params, callback) {
    return this.makeRequest({
        type: 'RpbGetBucketReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.setBucket = function (params, callback) {
    if (params.props) {
        params.props = quorum.convert(params.props);
    }

    return this.makeRequest({
        type: 'RpbSetBucketReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.resetBucket = function (params, callback) {
    return this.makeRequest({
        type: 'RpbResetBucketReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.getKeys = function (params, callback) {
    return this.makeRequest({
        type: 'RpbListKeysReq',
        params: params,
        expectMultiple: true,
        callback: callback
    });
};

RiakPBC.prototype.put = function (params, callback) {
    return this.makeRequest({
        type: 'RpbPutReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.get = function (params, callback) {
    return this.makeRequest({
        type: 'RpbGetReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.del = function (params, callback) {
    return this.makeRequest({
        type: 'RpbDelReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.mapred = function (params, callback) {
    function cb(err, reply) {
        if (err) {
            return callback(err);
        }

        delete reply.done;
        var phaseKeys = Object.keys(reply);
        var rows = [];
        var phase;

        phaseKeys.forEach(function (key) {
            phase = reply[key];
            phase.forEach(function (row) {
                rows.push(row);
            });
        });

        callback(undefined, rows);
    }

    return parseMapReduceStream(this.makeRequest({
        type: 'RpbMapRedReq',
        params: params,
        callback: callback ? cb : undefined,
        expectMultiple: true
    }));
};


RiakPBC.prototype.getCounter = function (params, callback) {
    return this.makeRequest({
        type: 'RpbCounterGetReq',
        params: params,
        callback: callback
    });
};


RiakPBC.prototype.updateCounter = function (params, callback) {
    return this.makeRequest({
        type: 'RpbCounterUpdateReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.getIndex = function (params, callback) {
    params.stream = true;

    return this.makeRequest({
        type: 'RpbIndexReq',
        params: params,
        expectMultiple: true,
        callback: callback
    });
};

RiakPBC.prototype.search = function (params, callback) {
    return this.makeRequest({
        type: 'RpbSearchQueryReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.getClientId = function (callback) {
    return this.makeRequest({
        type: 'RpbGetClientIdReq',
        params: null,
        callback: callback
    });
};

RiakPBC.prototype.setClientId = function (params, callback) {
    return this.makeRequest({
        type: 'RpbSetClientIdReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.getServerInfo = function (callback) {
    return this.makeRequest({
        type: 'RpbGetServerInfoReq',
        params: null,
        callback: callback
    });
};

RiakPBC.prototype.ping = function (callback) {
    return this.makeRequest({
        type: 'RpbPingReq',
        params: null,
        callback: callback
    });
};

RiakPBC.prototype.startTls = function (callback) {
    return this.makeRequest({
        type: 'RpbStartTls',
        params: null,
        callback: callback
    });
};

RiakPBC.prototype.auth = function (params, callback) {
    if (!this.connection.secure) {
        return callback(new Error('Cannot auth over non-secured connection'));
    }

    return this.makeRequest({
        type: 'RpbAuthReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.setBucketType = function (params, callback) {
    return this.makeRequest({
        type: 'RpbSetBucketTypeReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.getBucketType = function (params, callback) {
    return this.makeRequest({
        type: 'RpbGetBucketTypeReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.updateDtype = function (params, callback) {
    return this.makeRequest({
        type: 'DtUpdateReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.fetchDtype = function (params, callback) {
    return this.makeRequest({
        type: 'DtFetchReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.ykGetIndex = function (params, callback) {
    return this.makeRequest({
        type: 'RpbYokozunaIndexGetReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.ykPutIndex = function (params, callback) {
    return this.makeRequest({
        type: 'RpbYokozunaIndexPutReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.ykDeleteIndex = function (params, callback) {
    return this.makeRequest({
        type: 'RpbYokozunaIndexDeleteReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.ykPutSchema = function (params, callback) {
    return this.makeRequest({
        type: 'RpbYokozunaSchemaPutReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.ykGetSchema = function (params, callback) {
    return this.makeRequest({
        type: 'RpbYokozunaSchemaGetReq',
        params: params,
        callback: callback
    });
};

RiakPBC.prototype.connect = function (callback) {
    this.connection.connect(callback);
};

RiakPBC.prototype.disconnect = function () {
    if (this.task) {
        this.queue.unshift(this.task);
        this.task = undefined;
    }

    this.connection.disconnect();
};

exports.createClient = function (options) {
    return new RiakPBC(options);
};

function writableStream() {
    return new Stream.PassThrough({ objectMode: true });
}

function parseMapReduceStream(rawStream) {
    if (!rawStream) {
        return null;
    }

    var liner = new Stream.Transform({ objectMode: true });

    liner._transform = function (chunk, encoding, done) {
        var response = chunk.response;
        var json = JSON.parse(response);

        json.forEach(function (row) {
            liner.push(row);
        });

        done();
    };

    rawStream.on('error', function (err) {
        liner.emit('error', err);
    });

    rawStream.pipe(liner);
    return liner;
}
