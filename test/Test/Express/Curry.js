"use strict";

exports._write = function (resp, data) {
  return function () {
    resp.write(data);
  };
};
