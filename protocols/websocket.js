var ws = require('websocket');
var http = require('http');
var util = require('util');

function createServer(broker, callbacks) {
    var server = http.createServer();

    var wsServer = new ws.server({
        httpServer: server,
        autoAcceptConnections: true
    });

    wsServer.on('connect', function (webSocket) {
        var socket = webSocket.socket;
        
        var remoteAddress = webSocket.socket.address();
        util.log(util.format('websocket established to %s:%s', remoteAddress.address, remoteAddress.port));
        
        webSocket.on('message', function (message) {
            callbacks.debugDump(message.utf8Data);
            callbacks.frameReceived(message.utf8Data);
        });
        
        webSocket.on('close', function() {
            util.log(util.format('websocket to %s:%s terminated', remoteAddress.address, remoteAddress.port));
            callbacks.disconnected(webSocket);
        }.bind(this));
    }.bind(this));

    wsServer.listen = function() {
        const server = this.config.httpServer[0];
        return server.listen.apply(server, arguments);
    }

    wsServer.address = function() {
        const tokens = this.config.httpServer[0]._connectionKey.split(':')

        return {
            port: tokens[2],
            family: 'IPv'+tokens[0],
            address: tokens[1]
        }
    }

    return wsServer;
}

function sendMessage(socket, data) {
    if(data) {
        socket.sendUTF(data);
    }
}

module.exports = {
    createServer: createServer,
    sendMessage: sendMessage
}
