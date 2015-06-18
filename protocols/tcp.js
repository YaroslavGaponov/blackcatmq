var net = require('net');
var util = require('util');

function createServer(broker, callbacks) {
    return net.createServer(broker.serverOptions, function(socket) {            
        socket.setEncoding('utf8');
        socket.setKeepAlive(true);
        
        var remoteAddress = socket.address();
        util.log(util.format('server is connected to %s:%s', remoteAddress.address, remoteAddress.port));
        
        var data = '';
        socket.on('data', function(chunk) {
            callbacks.debugDump(chunk);
            
            data += chunk.toString();
            
            var frames = data.split(stomp.DELIMETER);
                            
            if (frames.length > 1) {
                data = frames.pop();                
                frames.forEach(callbacks.frameReceived);
            }
        });
        
        socket.on('end', function() {
            util.log(util.format('server is disconnected from %s:%s', remoteAddress.address, remoteAddress.port));
            callbacks.disconnected(socket);
        });
    });
}

function sendMessage(socket, data) {
    if (data) {
        if (socket && socket.writable) {
            socket.write(data.toString());
        }
    }
}

module.exports = {
    createServer: createServer,
    sendMessage: sendMessage
}
