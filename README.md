# UDP Log Catcher for Mongoose OS

## Overview

mos supports sending logs over UDP, a light-weight way of remote logging.

This utility catches these logs and splits them by device id into separate, daily logs files.

## Examples

`go build && ./mos_udp_log_catcher --listen-addr udp://:1234/ --log-dir /tmp/devlogs`
