#!/usr/bin/env node

/*
 blackcatmq
 copyright (c) 2012,2013 Yaroslav Gaponov <yaroslav.gaponov@gmail.com>

 changes:
    9 October 2013 Chris Flook - Expose blackcatmq for use as a module
*/

var DEBUG = false;

var util = require('util');
var fs = require('fs');

var stomp = require('./lib/stomp.js');

function getId() {
    return 'id' + Math.floor(Math.random() * 999999999999999999999);
}

var BlackCatMQ = function (config) {
    var self = this;

    if (self instanceof BlackCatMQ) {
        self.identifier = 'BlackCatMQ';

        self.port = config.port || 61613;
        self.host = config.host || '0.0.0.0';
        self.interval = config.interval || 50000;
        self.protocol = config.protocol || 'tcp';
        self.serverType = config.serverType || 'net';
        self.authType = config.authType || 'none';
        self.serverOptions = config.serverOptions || { 'allowHalfOpen': true };
        self.stompVersion = config.stompVersion ?
            config.stompVersion.slice(config.stompVersion.indexOf("1.")+2) || "0" : "0";

        self.sockets = {};
        self.subscribes = {};
        self.subscribesById = {};

        self.messages = { frame: {}, queue: [] };
        self.ack_list = [];

        self.transactions = {};

        self.auth = null;
        switch (self.authType.toLowerCase()) {
            case 'ldap':
                self.auth = new require('ldapauth')(config.authOptions);
                break;
        }

        self.protocolImpl = require('./protocols/'+self.protocol+'.js');

        self.server = self.protocolImpl.createServer(self, {
            frameReceived: function(socket, frameStr) {
                var frame = stomp.Frame(frameStr),
                    command = frame.command.toLowerCase();

                try {
                    if (typeof self.commands[command] === 'function') {
                        self.protocolImpl.sendMessage(socket, self.commands[command].call(self, socket, frame));
                    } else {
                        self.protocolImpl.sendMessage(socket, stomp.ServerFrame.ERROR('invalid parameters','command ' + frame.command + ' is not supported'));
                    }
                } catch (ex) {
                    util.log(ex.stack);
                    self.protocolImpl.sendMessage(socket, stomp.ServerFrame.ERROR(ex, 'unrecoverable error'));
                }
            },

            disconnected: function(socket) {
                self.commands.disconnect.call(self, socket, null);
            },

            debugDump: function(data) {
                if (DEBUG) {
                    if (!self.dumpFileName) {
                        self.dumpFileName = new Date().toString() + '.dat';
                    }
                    fs.appendFileSync('./dump/' + self.dumpFileName, data, encoding='utf8');
                }
            }
        });
    } else {
        return new BlackCatMQ(config);
    }
}

/*
 start server
*/
BlackCatMQ.prototype.start = function(callback) {
    var self = this;

    self.server.listen(self.port, self.host, function() {
        var addr = self.server.address();
        util.log(util.format("server is started on %s:%s ...", addr.address, addr.port));

        self.timerID = setInterval(function() { self.timer(); }, self.interval);

        if (callback && typeof callback === 'function') {
            return callback();
        }
    });
}

/*
 stop server
*/
BlackCatMQ.prototype.stop = function(callback) {
    var self = this;

    self.server.close(function() {
        util.log('server is stopped');

        clearInterval(self.timerID);

        if (callback && typeof callback === 'function') {
            return callback();
        }
    });
}

BlackCatMQ.prototype.deleteOwnSubscriptionsFromDestination = function(sessionId, subscriptions) {
    var pos = -1,
        subscription;

    for(var i=0; i<subscriptions.length; i++) {
        subscription = subscriptions[pos];
        if(subscription.sessionId == socket.sessionID) {
            pos = i;
            break;
        }
    }

    if (pos >= 0) {
        subscriptions.splice(pos, 1);
        if(subscription.id) {
            delete self.subscribesById["_"+subscription.id];
        }

        return subscription;
    }
}

BlackCatMQ.prototype.send = function(frame) {
    var self = this;

    if (!frame.header) {
        throw new Error('to send a frame the frame must have a header');
    }

    var destination = frame.header['destination'];
    if (!destination) {
        throw new Error('destination is a required argument to');
    }

    var subscriptions = self.subscribes[destination];

    if (subscriptions) {
        var messageID = getId();

        if (destination.indexOf('/queue/') === 0) {
            var subscription = subscriptions.pop(),
                session = subscription.sessionID;

            subscriptions.unshift(subscription);

            if (self.ack_list.indexOf(session) >= 0) {
                self.messages.frame[messageID] = frame;
                self.messages.queue.push(messageID);
            }
            self.protocolImpl.sendMessage(self.sockets[session], stomp.ServerFrame.MESSAGE(destination, messageID, frame.body, subscription.id));
        } else {
            subscriptions.forEach(function(subscription) {
                self.protocolImpl.sendMessage(self.sockets[subscription.sessionId], stomp.ServerFrame.MESSAGE(destination, messageID, frame.body, subscription.id));
            });
        }
    }
}

BlackCatMQ.prototype.commands = {
    connect: function(socket, frame) {
        var self = this;

        if (self.auth) {

            var login = frame.header['login']
            if (!login) {
                return stomp.ServerFrame.ERROR('connect error','login is required');
            }
            var passcode = frame.header['passcode']
            if (!passcode) {
                return stomp.ServerFrame.ERROR('connect error','passcode is required');
            }

            self.auth.authenticate(login, passocde, function(err, user) {
                if (err) {
                    return stomp.ServerFrame.ERROR('connect error','incorrect login or passcode');
                }

                var sessionID = getId();
                socket.sessionID = sessionID;
                self.sockets[sessionID] = socket;

                return stomp.ServerFrame.CONNECTED(sessionID, self.identifier);
            });
        }

        var sessionID = getId();
        socket.sessionID = sessionID;
        self.sockets[sessionID] = socket;

        return stomp.ServerFrame.CONNECTED(sessionID, self.identifier);
    },

    subscribe: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','you need connect before');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        if (!frame.header) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no header section');
        }

        var destination = frame.header['destination'];
        if (!destination) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no destination argument');
        }

        var id = frame.header['id'];
        if(!id && self.config.stompVersion !== "0") {
            return stomp.ServerFrame.ERROR('invalid parameters', 'there is no id argument');
        }

        if (frame.header['ack'] && frame.header['ack'] === 'client') {
            self.ack_list.push(socket.sessionID);
        }

        if (!self.subscribes[destination]) {
            self.subscribes[destination] = [];
        }

        var subscription = {
            id: id,
            sessionId: socket.sessionID,
            destination: destination
        };

        self.subscribes[destination].push(subscription);
        if(id) {
            self.subscribesById["_"+id] = subscription; //avoid 1000 length array for id "1000"
        }
    },

    unsubscribe: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','you need connect before');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        if (!frame.header) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no header section');
        }

        var destination = frame.header['destination'];
        var id = frame.header['id'];
        if(self.config.stompVersion !== "0") {
            if (!id) {
                return stomp.ServerFrame.ERROR('invalid parameters','you must specify an id to unsubscribe from');
            }
        } else {
            if (!destination) {
                return stomp.ServerFrame.ERROR('invalid parameters','you must specify a destination to unsubscribe from')
            }
        }

        var subscription;

        if(self.config.stompVersion !== "0") {
            subscription = self.subscribesById["_"+id];

            if(subscription) {
                delete self.subscribesById["_"+id];
                delete subscribes[subscription.destination];
            }
        } else {
            if (self.subscribes[destination]) {
                self.deleteOwnSubscriptionsFromDestination(socket.sessionID, self.subscribes[destination]);
            }
        }
    },

    send: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','you need connect before');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        if (!frame.header) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no header section');
        }

        var destination = frame.header['destination'];
        if (!destination) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no destination argument');
        }

        var transaction = frame.header['transaction'];
        if (transaction) {
            self.transactions[transaction].push(frame);
        }

        self.send(frame);
    },

    ack: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','you need connect before');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        if (!frame.header) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no header section');
        }

        var messageID = frame.header['message-id'];
        if (!messageID) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no message-id argument');
        }

        delete self.messages.frame[messageID];
        var pos = self.messages.queue.indexOf(messageID);
        if (pos >= 0) {
            self.messages.queue.splice(pos, 1);
        }

        return stomp.ServerFrame.RECEIPT(messageID);
    },

    disconnect: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','disconnect was called on a socket that was never connected or has already been disconnected');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        delete self.sockets[socket.sessionID];

        for (var destination in self.subscribes) {
            self.deleteOwnSubscriptionsFromDestination(socket.sessionID, self.subscribes[destination]);
        }

        var pos = self.ack_list.indexOf(socket.sessionID);
        if (pos >= 0) {
            self.ack_list.splice(pos, 1);
        }
    },

    begin: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','you need connect before');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        if (!frame.header) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no header section');
        }

        var trasaction = frame.header['transaction'];
        if (!trasaction) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no transaction argument');
        }

        self.transactions[transaction] = [];
    },

    commit: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','you need connect before');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        if (!frame.header) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no header section');
        }

        var trasaction = frame.header['transaction'];
        if (!trasaction) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no transaction argument');
        }

        delete self.transactions[transaction];
    },

    abort: function(socket, frame) {
        var self = this;

        if (!socket.sessionID) {
            return stomp.ServerFrame.ERROR('connect error','you need connect before');
        }

        if (self.sockets[socket.sessionID] !== socket) {
            return stomp.ServerFrame.ERROR('connect error','session is not correct');
        }

        if (!frame.header) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no header section');
        }

        var trasaction = frame.header['transaction'];
        if (!trasaction) {
            return stomp.ServerFrame.ERROR('invalid parameters','there is no transaction argument');
        }

        self.transactions[transaction].forEach(function(frame) {
            self.send(null, frame);
        });
        delete self.transactions[transaction];
    }
}


BlackCatMQ.prototype.timer = function() {
    var self = this;

    if (self.messages.queue.length > 0) {
        var messageID = self.messages.queue.shift();
        self.send(null, self.messages.frame[messageID]);
        delete self.messages.frame[messageID];
    }
}

if (require.main === module) {
    /*
     initalize & run server
    */
    fs.readFile(__dirname + '/blackcatmq.conf', 'utf8', function(err, data) {
        if (err) throw err;

        var config = JSON.parse(data);

        var server = new BlackCatMQ(config);
        if (server) {
            server.start();
        }

        process.once('uncaughtException', function(err) {
            util.debug('error:' + err + err.stack);
            if (server) {
                server.stop();
            }
        });

        process.once('exit', function() {
            if (server) {
                server.stop();
            }
        });

        process.once('SIGINT', function() {
            if (server) {
                server.stop();
            }
            console.log('Got SIGINT.  Press Control-c to exit.');
        });
    });
}

BlackCatMQ.create = function(config){
    return new BlackCatMQ(config || {});
}

module.exports = BlackCatMQ;
