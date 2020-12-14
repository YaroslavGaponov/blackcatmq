BlackCatMq
========

## Overview

BlackCatMq - simple STOMP messages broket (aka STOMP server) in node.js

## Installation

`npm install blackcatmq`

`git clone https://github.com/YaroslavGaponov/blackcatmq.git`

## Adjust

`./configure.js`

## Run

` ./blackcatmq.js`

## Features

  Support authentication through LDAP server

  Support TLS/SSL
  
  Support Custom Headers


## Docker

```sh
docker pull yaroslavgaponov/blackcatmq
docker run -p 61613:61613 yaroslavgaponov/blackcatmq
```

## Contributors
Yaroslav Gaponov (yaroslav.gaponov -at - gmail.com)
