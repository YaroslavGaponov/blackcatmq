
/*
 blackcatmq
 Copyright (c) 2012,2013 Yaroslav Gaponov <yaroslav.gaponov@gmail.com>
*/

var DELIMETERS = {
    linux: '\x00',
    darwin: '\x00'
}


var DELIMETER = module.exports.DELIMETER = DELIMETERS[require('os').platform()] || DELIMETERS.linux;

var Frame = module.exports.Frame = function (data) {

    if (this instanceof Frame) {

        if (typeof data === 'object') {
            for(key in data) {
                this[key] = data[key];
            }
        } else {
            var cmd_hdr_sections = data.split('\x0a\x0a')[0];
            var cmd_hdr_lines = cmd_hdr_sections.split('\x0a')

            var header = {};
            for (var i = 1; i < cmd_hdr_lines.length; i++) {
                var key_value = cmd_hdr_lines[i].split(':');
                header[key_value[0]] = key_value[1];
            }

            this.command = cmd_hdr_lines[0];
            this.header = header;
            this.body =  data.slice(cmd_hdr_sections.length + 2);
        }
    } else {
        return new Frame(data);
    }
}


Frame.prototype.toString = function () {
    var data = this.command.toUpperCase() + '\x0a';

    for (var key in this.header) {
        if(this.header[key] !== undefined) {
            data += key + ':' + this.header[key] + '\x0a';
        }
    }

    data += '\x0a';

    if (this.body) {
        data += this.body.toString();
    }

    data += DELIMETER;

    return data;
}


module.exports.ServerFrame = {
    CONNECTED: function (session, identifier) { return Frame({ command: 'CONNECTED', header: { session: session, identifier: identifier } });  },
    MESSAGE: function (frame) { return Frame( { command: 'MESSAGE', header: frame.header, body: frame.body } ); },
    RECEIPT: function (receipt) { return Frame( { command: 'RECEIPT', header: { 'receipt-id': receipt } }); },
    ERROR: function (message, description) { return Frame( { command: 'ERROR', header: { message: message }, body: description }); }
};



