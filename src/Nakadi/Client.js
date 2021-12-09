'use strict'

exports.removeRequestTimeout = function (request) {
    return function () {
        request.setTimeout(0);
        return;
    }
}
