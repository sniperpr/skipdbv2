#!/bin/sh 

COUNTER=0

V="your are the best"
K="hello"
while [  $COUNTER -lt 15000 ]; do
    #echo The counter is $V$COUNTER
    ../build/bin/dbus set $K$COUNTER=$V$COUNTER
    #../build/bin/dbus get $K$COUNTER
    let COUNTER=COUNTER+1 
done
