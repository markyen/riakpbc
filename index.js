var net = require('net'),
    protobuf = require('protobuf.js'),
    async = require('async');

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
    '28': 'RpbSearchQueryResp'
};
Object.keys(messageCodes).forEach(function (key) {
    messageCodes[messageCodes[key]] = Number(key);
});

function RiakPBC(options) {
    var self = this;
    options = options || {};
    self.host = options.host || 'localhost';
    self.port = options.port || 8087;
    self.bucket = options.bucket || undefined;
    self.translator = protobuf.loadSchema('./spec/riak_kv.proto');
    self.client = new net.Socket();
    self.connected = false;
    self.client.on('end', self.disconnect);
    self.client.on('error', self.disconnect);
    self.client.on('timeout', self.disconnect);
    self.queue = async.queue(function (task, callback) {
        var mc, reply = {};
        var checkReply = function (chunk) {
            var decoded;
            splitPacket(chunk).forEach(function (packet) {
                mc = messageCodes['' + packet.readInt8(0)];
                decoded = self.translator.decode(mc, packet.slice(1));
                if (decoded.response) decoded.response = JSON.parse(decoded.response);
                reply = _merge(reply, decoded);
                if (!task.expectMultiple || reply.done || mc === 'RpbErrorResp') {
                    self.client.removeListener('data', checkReply);
                    if (mc === 'RpbErrorResp') {
                        task.callback(reply);
                    } else {
                        task.callback(null, reply);
                    }
                    callback();
                }
            });
        }
        self.client.on('data', checkReply);
        self.client.write(task.message);
    }, 1);

    function splitPacket(pkt) {
        var ret = [];
        while (pkt.length > 0) {
            var len = pkt.readUInt32BE(0),
                buf = new Buffer(len);

            pkt.copy(buf, 0, 4, len + 4);
            ret.push(buf);
            pkt = pkt.slice(len + 4);
        }
        return ret;
    }
};

function _merge(obj1, obj2) {
    var obj = {};
    Object.keys(obj1).forEach(function (key) {
        if (Array.isArray(obj1[key])) {
            if (!obj[key]) obj[key] = [];
            obj[key] = obj[key].concat(obj1[key]);
        } else {
            obj[key] = obj1[key];
        }
    });
    Object.keys(obj2).forEach(function (key) {
        if (Array.isArray(obj2[key])) {
            if (!obj[key]) obj[key] = [];
            obj[key] = obj[key].concat(obj2[key]);
        } else {
            obj[key] = obj2[key];
        }
    });
    return obj;
};

function _dedupe(arr) {
    var ret = [];
    arr.forEach(function (item) {
        if (!~ret.indexOf(item)) ret.push(item);
    });
    return ret;
};

RiakPBC.prototype.makeRequest = function (type, data, callback, expectMultiple) {
    var self = this,
        reply = {},
        buffer = this.translator.encode(type, data),
        message = new Buffer(buffer.length + 5);

    message.writeUInt32BE(buffer.length + 1, 0);
    message.writeInt8(messageCodes[type], 4);
    buffer.copy(message, 5);
    this.connect(function () {
        self.queue.push({ message: message, callback: callback, expectMultiple: expectMultiple });
    });
};

RiakPBC.prototype.getBuckets = function (callback) {
    this.makeRequest('RpbListBucketsReq', null, callback);
};

RiakPBC.prototype.getBucket = function (params, callback) {
    if (typeof params === 'function') {
        callback = params;
        params = {};
    }
    var data = {};
    data.bucket = params.bucket || this.bucket;
    this.makeRequest('RpbGetBucketReq', data, callback);
};

RiakPBC.prototype.setBucket = function (params, callback) {
    if (typeof params === 'function') {
        callback = params;
        params = {};
    }
    var data = { props: {} };
    data.bucket = params.bucket || this.bucket;
    data.props.allow_mult = params.allow_mult;
    data.props.n_val = params.n_val;
    this.makeRequest('RpbSetBucketReq', data, callback);
};

RiakPBC.prototype.getKeys = function (params, callback) {
    var self = this;
    var data = {};
    data.bucket = params.bucket || this.bucket;
    if (params.index) {
        var results = [];
        async.forEach(Object.keys(params.index), function (key, cb) {
            data.index = key + '_bin';
            data.qtype = 0;
            data.key = '' + params.index[key];
            self.makeRequest('RpbIndexReq', data, function (err, reply) {
                if (err) return callback(err, reply);
                if (reply.keys) results.push(reply.keys);
                cb();
            });
        }, function (err) {
            if (results.length > 0) {
                results = _dedupe(results[0].filter(function (key) {
                    var intersects = true;
                    for (var i = 1; i < results.length; i++) {
                        if (!~results[i].indexOf(key)) intersects = false;
                    }
                    return intersects;
                }));
            }
            callback(null, { keys: results });
        });
    } else {
        this.makeRequest('RpbListKeysReq', data, callback, true);
    }
};

RiakPBC.prototype.put = function (params, callback) {
    var self = this;
    var data = { content: {} };
    data.bucket = params.bucket || this.bucket;
    if (typeof params.data === 'object') params.data = JSON.stringify(params.data);
    data.content.value = params.data;
    data.content.content_type = params.contentType || 'application/json';
    data.return_body = params.returnbody;
    if (params.index && typeof params.index === 'object') {
        data.content.indexes = [];
        var index, value;
        Object.keys(params.index).forEach(function (key) {
            index = key + '_bin';
            value = '' + params.index[key];
            data.content.indexes.push({ key: index, value: value });
        });
    }
    if (params.key) {
        data.key = params.key;
        this.makeRequest('RpbGetReq', { bucket: data.bucket, key: data.key }, function (err, reply) {
            if (reply && reply.vclock) data.vclock = reply.vclock;
            self.makeRequest('RpbPutReq', data, callback);
        });
    } else {
        this.makeRequest('RpbPutReq', data, callback);
    }
};

RiakPBC.prototype.get = function (params, callback) {
    var self = this;
    var data = {};
    data.bucket = this.bucket || params.bucket;
    if (params.key) {
        data.key = params.key;
        this.makeRequest('RpbGetReq', data, function (err, reply) {
            if (err) return callback(err);
            if (Object.keys(reply).length === 0) return callback({ errmsg: 'not found', errcode: 0 });
            reply.content = reply.content.filter(function (item) {
                return item.deleted ? false : true;
            });
            callback(null, reply);
        });
    } else if (params.index) {
        var results = [];
        async.forEach(Object.keys(params.index), function (key, cb) {
            data.index = key + '_bin';
            data.qtype = 0;
            data.key = params.index[key];
            self.makeRequest('RpbIndexReq', data, function (err, reply) {
                if (err || !reply.keys) return callback(err, reply);
                results.push(reply.keys);
                cb();
            });
        }, function (err) {
            var final = [];
            results = _dedupe(results[0].filter(function (key) {
                var intersects = true;
                for (var i = 1; i < results.length; i++) {
                    if (!~results[i].indexOf(key)) intersects = false;
                }
                return intersects;
            }));
            async.forEach(results, function (key, cb) {
                self.makeRequest('RpbGetReq', { bucket: data.bucket, key: key }, function (err, reply) {
                    reply.content = reply.content.filter(function (item) {
                        return item.deleted ? false : true;
                    });
                    final.push(reply);
                    cb(err);
                });
            }, function (err) {
                if (err) return callback(err);
                if (final.length === 0) return callback({ errmsg: 'not found', errcode: 0 });
                callback(null, final);
            });
        });
    }
};

RiakPBC.prototype.modify = function (params, callback) {
    var self = this;
    var data = { key: params.key };
    data.bucket = params.bucket || this.bucket;
    this.get(params, function (err, reply) {
        if (err) return callback(err);
        if (Object.keys(reply).length === 0) return callback({ errmsg: 'not found', errcode: 0 });
        var newobj = { content: reply.content[0], vclock: reply.vclock, bucket: data.bucket, key: data.key };
        if (params.transform) newobj.content.value = params.transform(newobj.content.value);
        if (params.index) {
            var current = [], key, position;
            if (!newobj.content.indexes) newobj.content.indexes = [];
            newobj.content.indexes.forEach(function (index) {
                current.push(index.key);
            });
            Object.keys(params.index).forEach(function (index) {
                key = index + '_bin';
                position = current.indexOf(key);
                if (~position && params.index[index]) newobj.content.indexes[position].value = params.index[index];
                if (~position && params.index[index] === undefined) newobj.content.indexes.splice(position, 1);
                if (!~position && params.index[index]) newobj.content.indexes.push({ key: key, value: params.index[index] });
            });
        }
        self.makeRequest('RpbPutReq', newobj, callback);
    });
};

RiakPBC.prototype.del = function (params, callback) {
    var data = { key: params.key };
    data.bucket = this.bucket || params.bucket;
    this.makeRequest('RpbDelReq', data, callback);
};

RiakPBC.prototype.mapred = function (params, callback) {
    var self = this;
    var data = { request: { query: [] }, content_type: 'application/json' },
        bucket = params.bucket || this.bucket;

    if (params.map) {
        if (typeof params.map === 'string') {
            data.request.query.push({ map: { name: params.map, language: 'javascript' } });
        } else if (typeof params.map === 'object') {
            var map = {};
            if (params.map.name) map.name = params.map.name;
            if (params.map.source) map.source = params.map.source;
            if (params.map.arg) map.arg = params.map.arg;
            if (params.map.language) map.language = params.map.language;
            if (params.map.keep) map.keep = params.map.keep;
            data.request.query.push({ map: map });
        } else if (typeof params.map === 'function') {
            data.request.query.push({ map: { source: params.map.toString(), language: 'javascript' } });
        }
    }

    if (params.reduce) {
        if (typeof params.reduce === 'string') {
            data.request.query.push({ reduce: { name: params.reduce, language: 'javascript' } });
        } else if (typeof params.reduce === 'object') {
            var reduce = {};
            if (params.reduce.name) reduce.name = params.reduce.name;
            if (params.reduce.source) reduce.source = params.reduce.source;
            if (params.reduce.arg) reduce.arg = params.reduce.arg;
            if (params.reduce.language) reduce.language = params.reduce.language;
            if (params.reduce.keep) reduce.keep = params.reduce.keep;
            data.request.query.push({ reduce: reduce });
        } else if (typeof params.reduce === 'function') {
            data.request.query.push({ reduce: { source: params.reduce.toString(), language: 'javascript' } });
        }
    }

    if (!params.key && !params.index) {
        data.request.inputs = bucket;
        //console.log('BUCKET WIDE:', require('util').inspect(data, false, null));
        data.request = JSON.stringify(data.request);
        this.makeRequest('RpbMapRedReq', data, callback, true);
    } else if (params.key) {
        if (!Array.isArray(params.key)) params.key = [params.key];
        data.request.inputs = params.key.map(function (key) {
            return [bucket, key];
        });
        //console.log('MANUAL KEYS:', require('util').inspect(data, false, null));
        data.request = JSON.stringify(data.request);
        this.makeRequest('RpbMapRedReq', data, callback, true);
    } else if (params.index) {
        if (Object.keys(params.index).length === 1) {
            var key = Object.keys(params.index)[0];
            data.request.inputs = { bucket: bucket, index: key + '_bin', key: params.index[key] };
            //console.log('SINGLE INDEX:', require('util').inspect(data, false, null));
            data.request = JSON.stringify(data.request);
            this.makeRequest('RpbMapRedReq', data, callback, true);
        } else {
            data.request.inputs = [];
            this.getKeys({ bucket: bucket, index: params.index }, function (err, reply) {
                if (err) return callback(err);
                reply.keys.forEach(function (key) {
                    data.request.inputs.push([bucket, key]);
                });
                //console.log('MULTIPLE KEYS:', require('util').inspect(data, false, null));
                data.request = JSON.stringify(data.request);
                self.makeRequest('RpbMapRedReq', data, callback, true);
            });
        }
    }
};

RiakPBC.prototype.getIndexes = function (params, callback) {
    this.get(params, function (err, reply) {
        if (err) return callback(err);
        var indexes = {}, name;
        reply.content[0].indexes.forEach(function (index) {
            name = index.key.replace(/_bin$|_int$/, '');
            indexes[name] = index.value;
        });
        callback(null, indexes);
    });
    //this.makeRequest('RpbIndexReq', params, callback);
};

RiakPBC.prototype.search = function (params, callback) {
    this.makeRequest('RpbSearchQueryReq', params, callback);
};

RiakPBC.prototype.getClientId = function (callback) {
    this.makeRequest('RpbGetClientIdReq', null, callback);
};

RiakPBC.prototype.setClientId = function (params, callback) {
    this.makeRequest('RpbSetClientIdReq', params, callback);
};

RiakPBC.prototype.getServerInfo = function (callback) {
    this.makeRequest('RpbGetServerInfoReq', null, callback);
};

RiakPBC.prototype.ping = function (callback) {
    this.makeRequest('RpbPingReq', null, callback);
};

RiakPBC.prototype.connect = function (callback) {
    if (this.connected) return callback();
    var self = this;
    self.client = net.connect(self.port, self.host, function () {
        self.connected = true;
        callback();
    });
};

RiakPBC.prototype.disconnect = function () {
    if (this.connected) {
        this.connected = false;
        this.client.end();
    }
};

exports.createClient = function (options) {
    return new RiakPBC(options);
};
