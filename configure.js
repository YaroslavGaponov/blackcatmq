#!/usr/bin/env node

var util = require('util');
var fs = require('fs');
var readline = require('readline');

var config = JSON.parse(fs.readFileSync('./blackcatmq.conf', 'utf8'));

var rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});


tasks = [
    function(next) {
        rl.question(util.format("host {%s} :", config.host), function(answer) {    
            config.host = answer !== '' ? answer : config.host;        
            next();
        });
    },
    
    function(next) {
        rl.question(util.format("port {%s} :", config.port), function(answer) {    
            config.port = answer !== '' ? answer : config.port;        
            next();
        });
    },
    
    
    function(next) {
        rl.question(util.format("interval {%s} :", config.interval), function(answer) {    
            config.interval = answer !== '' ? answer : config.interval;        
            next();
        });
    },
    
    function(next) {
        rl.question(util.format("serverType {%s} [net,tls] :", config.serverType), function(answer) {    
            config.serverType = answer !== '' ? answer : config.serverType;
            switch (config.serverType) {
                case 'net':
                    config.serverOptions = {"allowHalfOpen":true};
                    break;
                case 'tls':
                    config.serverOptions = { "key": "", "cert": "", "ca": ""};
                    break;
            }
            next();
        });
    },

    function(next) {
        if (config.serverType === 'net') {
            rl.question(util.format("serverOptions.allowHalfOpen {%s} :", config.serverOptions.allowHalfOpen), function(answer) {    
                config.serverOptions.allowHalfOpen = answer !== '' ? answer : config.serverOptions.allowHalfOpen;  
                next();
            });
        } else {
            next();
        }
    },
    
    
    function(next) {
        if (config.serverType === 'tls') {
            rl.question("serverOptions.key  :", function(answer) {    
                config.serverOptions.key = answer;
                next();
            });
        } else {
            next();
        }
    },

    function(next) {
        if (config.serverType === 'tls') {
            rl.question("serverOptions.cert  :", function(answer) {    
                config.serverOptions.cert = answer;
                next();
            });
        } else {
            next();
        }
    },
    

    function(next) {
        if (config.serverType === 'tls') {
            rl.question("serverOptions.ca  :", function(answer) {    
                config.serverOptions.ca = answer;
                next();
            });
        } else {
            next();
        }
    },

    
    function(next) {
        rl.question(util.format("authType {%s} [none, ldap] :", config.authType), function(answer) {    
            config.authType = answer !== '' ? answer : config.authType;
            if (config.authType === 'ldap' && !config.authOptions) {
                config.authOptions = {
                    "url":"ldaps://ldap.example.com:663",
                    "adminDn":"uid=myadminusername,ou=users,o=example.com",
                    "adminPassword":"mypassword",
                    "searchBase":"ou=users,o=example.com",
                    "searchFilter":"(uid={{username}})",
                    "cache":true
                };
            } else {
                delete config.authOptions;
            }
            next();
        });
    },
    
    function(next) {
        if (config.authType === 'ldap') {
            rl.question(util.format("authOptions.url {%s}  :", config.authOptions.url), function(answer) {    
                config.authOptions.url = answer !== '' ? answer : config.authOptions.url;        
                next();
            });
        } else {
            next();
        }
    },

    function(next) {
        if (config.authType === 'ldap') {
            rl.question(util.format("authOptions.adminDn {%s}  :", config.authOptions.adminDn), function(answer) {    
                config.authOptions.adminDn = answer !== '' ? answer : config.authOptions.adminDn;        
                next();
            });
        } else {
            next();
        }
    },    
    
    function(next) {
        if (config.authType === 'ldap') {
            rl.question(util.format("authOptions.adminPassword {%s}  :", config.authOptions.adminPassword), function(answer) {    
                config.authOptions.adminPassword = answer !== '' ? answer : config.authOptions.adminPassword;        
                next();
            });
        } else {
            next();
        }
    },

    function(next) {
        if (config.authType === 'ldap') {
            rl.question(util.format("authOptions.searchBase {%s}  :", config.authOptions.searchBase), function(answer) {    
                config.authOptions.searchBase = answer !== '' ? answer : config.authOptions.searchBase;        
                next();
            });
        } else {
            next();
        }
    },
    

    function(next) {
        if (config.authType === 'ldap') {
            rl.question(util.format("authOptions.searchFilter {%s}  :", config.authOptions.searchFilter), function(answer) {    
                config.authOptions.searchFilter = answer !== '' ? answer : config.authOptions.searchFilter;        
                next();
            });
        } else {
            next();
        }
    },


    function(next) {
        if (config.authType === 'ldap') {
            rl.question(util.format("authOptions.cache {%s}  :", config.authOptions.cache), function(answer) {    
                config.authOptions.cache = answer !== '' ? answer : config.authOptions.cache;        
                next();
            });
        } else {
            next();
        }
    },

        
    function() {
        rl.question("save to file {blackcatmq.conf} :", function(answer) {    
            fs.writeFile(answer !== '' ? answer : 'blackcatmq.conf', JSON.stringify(config), 'utf8', function(err) {
                if (err) console.log('error: ' + err)
                    else console.log('done successful');
                rl.close();
            });
        });
    }    
];



(function step() {
    tasks.shift()(step);
}());

