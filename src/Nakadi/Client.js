'use strict'

exports.setRequestTimeout = function (timeout) {
    return function (request) {
        return function () {
            request.setTimeout(timeout, function() {
                request.destroy(); // this will close the connection and cause the request 'error' handler to be invoked
            });
            return;
        }
    }
}

exports.requestOnError = function (request) {
    return function (handler) {
        return function () {
            request.on("error", function(err) {
                handler(err)();
            });
        }
    }
}
