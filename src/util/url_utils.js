/* Copyright (C) 2016 NooBaa */
'use strict';

var _ = require('lodash');
var url = require('url');
var querystring = require('querystring');

var QUICK_PARSE_REGEXP = /^\s*(\w+:)?(\/\/)?(([^:/[\]]*)|\[([a-fA-F0-9:.]*)\])?(:\d*)?(\/[^?#]*)?(\?[^#]*)?(#.*)?\s*$/;

/**
 * parse url string much faster than url.parse() - reduce the time to 1/10.
 * this is handy when url parsing is part of incoming request handling and called many times per second.
 * !!! - quick_parse is not conforming to the way url.parse works. see comment at the beginning of the function.
 *
 * on MacAir url.parse() runs ~110,000 times per second while consuming 100% cpu,
 * so url.parse() can be heavy for high RPM server.
 * quick_parse() runs ~1,000,000 times per second.
 * see benchmark() function below.
 *
 */
function quick_parse(url_string, parse_query_string) {
    // we perfrom toLowerCase on the entire url_string even though it is not conforming to url.parse implementation
    // we do it to avoid complexity, and since we use quick_parse on very specific places it doesn't matter for now.
    // we need to review it again if neccessary.
    url_string = url_string.toLowerCase();
    var match = url_string.match(QUICK_PARSE_REGEXP);
    var u = new url.Url();
    if (!match) return u;
    u.href = url_string;
    u.protocol = match[1] || null;
    u.slashes = match[2] ? true : null;
    u.hostname = match[4] || match[5] || '';
    u.port = (match[6] && match[6].slice(1)) || null;
    u.host = (match[3] || '') + (match[6] || '');
    u.pathname = match[7] || null;
    u.search = match[8] || '';
    u.query = match[8] ? match[8].slice(1) : null;
    u.path = (match[7] || '') + (match[8] || '');
    u.hash = match[9] || null;
    // u.auth = null;
    if (parse_query_string && u.query) {
        u.query = querystring.parse(u.query);
    } else {
        u.query = {};
    }
    return u;
}

function construct_url(def) {
    const { protocol = 'http', hostname, port } = def;
    if (!hostname) {
        throw new Error('Invalid definition, hostname is mandatory');
    }

    return new URL(port ?
        `${protocol || 'http'}://${hostname}:${port}` :
        `${protocol || 'http'}://${hostname}`
    );
}

function benchmark() {
    var testing_url = process.argv[2] || "http://localhost:4545/";
    var url_parse_res = url.parse(testing_url, true);
    var quick_parse_res = quick_parse(testing_url, true);
    console.log('\nurl.parse("' + testing_url + '") = ', url_parse_res);
    console.log('\nquick_parse("' + testing_url + '") = ', quick_parse_res);
    console.log(' ');
    _.forIn(url_parse_res, function(v1, k) {
        var v2 = quick_parse_res[k];
        if (!_.isEqual(v1, v2)) {
            console.log('!!! Bad value quick_parse()',
                k + ': ' + JSON.stringify(v2),
                'expected', JSON.stringify(v1));
        }
    });
    var url_parse_fmt = url.format(url_parse_res);
    var quick_parse_fmt = url.format(quick_parse_res);
    if (url_parse_fmt !== testing_url) {
        console.log('!!! Bad format(url.parse) =', url_parse_fmt, 'expected', testing_url);
    }
    if (quick_parse_fmt !== testing_url) {
        console.log('!!! Bad format(quick_parse) =', quick_parse_fmt, 'expected', testing_url);
    }
    var tests = [
        function test_url_parse() {
            return url.parse(testing_url, true);
        },
        function test_quick_parse() {
            return quick_parse(testing_url, true);
        }
    ];
    for (var t = 0; t < tests.length; ++t) {
        var test = tests[t];
        console.log('\nbenchmarking', test.name, '...');
        var count = 0;
        var start = Date.now();
        var now = start;
        var last_print = start;
        var last_count = 0;
        var speed;
        do {
            for (var i = 0; i < 5000; ++i) test();
            count += 5000;
            now = Date.now();
            if (now - last_print > 1000) {
                speed = ((count - last_count) * 1000 / (now - last_print)).toFixed(0);
                console.log('\tcurrent times per second:', speed);
                last_print = now;
                last_count = count;
            }
        } while (now - start < 5000);
        speed = (count * 1000 / (now - start)).toFixed(0);
        console.log('\tOVERALL times per second:', speed);
    }
    console.log('\ndone.\n');
}

exports.quick_parse = quick_parse;
exports.construct_url = construct_url;

if (require.main === module) {
    benchmark();
}
