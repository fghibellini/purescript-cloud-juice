'use strict'

// const bufsize = 1024*1024*50;
// const buffer = Buffer.allocUnsafe(bufsize);

exports.allocUnsafeImpl = function(bufsize) {
    return function() { return Buffer.allocUnsafe(bufsize); };
}

exports.splitAtNewlineImpl = function(buffer, bufsize, errCallback, batchCallback) {
    return function () {
        var failed = false; // once an error is encountered no more chunks are processed
        var pos = 0;
        return function (chunk) {
            return function() {
                if (failed) {
                    return; // do nothing
                } else if(chunk.length + pos > bufsize) {
                    failed = true;
                    errCallback(new Error("Exceeded maximum buffer size of " + bufsize / 1024 / 1024 + " MiB"))()
                } else {
                    for (var i = 0; i < chunk.length; i++) {
                        if (chunk[i] == 10) { // newline
                            let toPush = buffer.toString('utf-8', 0, pos)
                            let parsed;
                            try {
                                parsed = JSON.parse(toPush)
                            } catch(err) {
                                failed = true;
                                return errCallback(new Error(`Got error: ${err.message} while trying to parse line from Nakadi: ${toPush}`))()
                            }
                            pos = 0;
                            batchCallback(parsed)();
                        } else {
                            buffer[pos] = chunk[i];
                            pos++;
                        }
                    }
                }
            }
        }
    }
}

exports.newHttpsAgent = function() {
    return new require('https').Agent({
        // this is the `keepAlive` configuration option of NodeJS `http.Agent`,
        // it has nothing to do with the HTTP KeepAlive mechanisms
        // see https://nodejs.org/api/http.html#new-agentoptions
        keepAlive: true,
        maxSockets: 1
    })
};

exports.newHttpAgent = function() {
    return new require('http').Agent({
        // this is the `keepAlive` configuration option of NodeJS `http.Agent`,
        // it has nothing to do with the HTTP KeepAlive mechanisms
        // see https://nodejs.org/api/http.html#new-agentoptions
        keepAlive: true,
        maxSockets: 1
    })
};

exports.destroyAgent = function(agent) {
  return function() { agent.destroy(); };
}
