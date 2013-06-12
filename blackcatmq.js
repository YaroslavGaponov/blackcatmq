#!/usr/bin/env node

/*
 blackcatmq
 copyright (c) 2012,2013 Yaroslav Gaponov <yaroslav.gaponov@gmail.com>
*/

var DEBUG = false;

var util = require('util');
var fs = require('fs');

var stomp = require('./lib/stomp.js');


function sender(socket, data) {
    if (data) {
        if (socket && socket.writable) {
            socket.write(data.toString());
        }
    }
}

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
        
        self.sockets = {};
        self.subscribes = {};
        
        self.messages = { frame: {}, queue: [] };
        self.ack_list = [];
        
        self.transactions = {};
            
        self.auth = null;
        switch (config.authType.toLowerCase()) {
            case 'ldap':
                self.auth = new require('ldapauth')(config.authOprions);
                break;                
        }
        
        self.server = require(config.serverType).createServer(config.serverOptions, function(socket) {            
            socket.setEncoding('utf8');
            socket.setKeepAlive(true);
            
            var remoteAddress = socket.address();
            util.log(util.format('server is connected to %s:%s', remoteAddress.address, remoteAddress.port));
            
            var data = '';
            socket.on('data', function(chunk) {
                
                if (DEBUG) {
                    if (!self.dumpFileName) {
                        self.dumpFileName = new Date().toString() + '.dat';
                    }
                    fs.appendFileSync('./dump/' + self.dumpFileName, chunk, encoding='utf8');
                }
                
                data += chunk.toString();
                
                var frames = data.split(stomp.DELIMETER);
                                
                if (frames.length > 1) {
                    data = frames.pop();                
                    frames.forEach(function(_frame) {
                        var frame = stomp.Frame(_frame);
                        try {                            
                            if (DEBUG) {
                                util.log(util.inspect(frame));
                            }
                            
                            if (self[frame.command.toLowerCase()] && typeof self[frame.command.toLowerCase()] === 'function') {
                                sender(socket, self[frame.command.toLowerCase()].call(self, socket, frame));
                            } else {
                                sender(socket, stomp.ServerFrame.ERROR('invalid parameters','command ' + frame.command + ' is not supported'));
                            }
                        } catch (ex) {
                            sender(socket, stomp.ServerFrame.ERROR(ex, ex.stack));
                        }
                    });
                }   
                
            });
            
            socket.on('end', function() {
                util.log(util.format('server is disconnected from %s:%s', remoteAddress.address, remoteAddress.port));
                self.disconnect(socket, null);
            });
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

/*
 STOMP command  -> connect
*/
BlackCatMQ.prototype.connect = function(socket, frame) {
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
}

/*
 STOMP command -> subscribe
*/
BlackCatMQ.prototype.subscribe = function(socket, frame) {
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
        
    if (frame.header['ack'] && frame.header['ack'] === 'client') {
        self.ack_list.push(socket.sessionID);
    }
    
    if (self.subscribes[destination]) {
        self.subscribes[destination].push(socket.sessionID);    
    } else {
        self.subscribes[destination] = [socket.sessionID];   
    }
}

/*
 STOMP command -> unsubsctibe
*/
BlackCatMQ.prototype.unsubscribe = function(socket, frame) {
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
    
    if (self.subscribes[destination]) {
        var pos = self.subscribes[destination].indexOf(socket.sessionID);
        if (pos >= 0) {
            self.subscribes[destination].splice(pos, 1);   
        }        
    }
}

/*
 STOMP command -> send
*/
BlackCatMQ.prototype.send = function(socket, frame) {
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
    
    if (self.subscribes[destination]) {
                
        var messageID = getId();
                
        if (destination.indexOf('/queue/') === 0) {
            var session =  self.subscribes[destination].pop();
            self.subscribes[destination].unshift(session);
                        
            if (self.ack_list.indexOf(session) >= 0) {
                self.messages.frame[messageID] = frame;
                self.messages.queue.push(messageID);
            }            
            sender(self.sockets[session], stomp.ServerFrame.MESSAGE(destination, messageID, frame.body));            
        } else {
            self.subscribes[destination].forEach(function(session) {
                sender(self.sockets[session], stomp.ServerFrame.MESSAGE(destination, messageID, frame.body));                
            });    
        }
    }
}

/*
 STOMP command -> ack
*/
BlackCatMQ.prototype.ack = function(socket, frame) {
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
}


/*
 STOMP command -> disconnect
*/
BlackCatMQ.prototype.disconnect = function(socket, frame) {
    var self = this;

    if (!socket.sessionID) {
        return stomp.ServerFrame.ERROR('connect error','you need connect before');
    }

    if (self.sockets[socket.sessionID] !== socket) {
        return stomp.ServerFrame.ERROR('connect error','session is not correct');
    }
    
    delete self.sockets[socket.sessionID];
    
    for (var destination in self.subscribes) {
        var pos = self.subscribes[destination].indexOf(socket.sessionID);
        if (pos >= 0) {
            self.subscribes[destination].splice(pos, 1);   
        }
    }
    
    var pos = self.ack_list.indexOf(socket.sessionID);
    if (pos >= 0) {
        self.ack_list.splice(pos, 1);
    }
}

/*
 STOMP command -> begin
*/
BlackCatMQ.prototype.begin = function(socket, frame) {
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
}

/*
 STOMP command -> commit
*/
BlackCatMQ.prototype.commit = function(socket, frame) {
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
}

/*
 STOMP command -> abort
*/
BlackCatMQ.prototype.abort = function(socket, frame) {
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


/*
 periodic task - return of lost messages 
*/
BlackCatMQ.prototype.timer = function() {
    var self = this;
    
    if (self.messages.queue.length > 0) {
        var messageID = self.messages.queue.shift();
        self.send(null, self.messages.frame[messageID]);
        delete self.messages.frame[messageID];
    }
}


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


