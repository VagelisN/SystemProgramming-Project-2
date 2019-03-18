#!/bin/bash

if [ $# -lt 4 -o $# -gt 5 ] 
then
	echo "Wrong number of args given!"
	exit
fi

n=-1

if [ $1 = "-l" ]
then
	 path="$2"
	 com="$4"
	 if [ $# -eq 5 ]
	 then
	 	n=$5
	 fi

else
	
	com="$2"
	if [ $# -eq 5 ]
	then
	 	n=$3
	fi
	path="$5"
fi

if [ $com = "list" ]
then
	ls -l $path
elif [ $com = "purge" ]
then
	rm -rf $path
elif [ $com = "size" ]
then
	if [ n = -1 ]
	then
		du $path | sort -n
	else
		du $path | sort -n | head -n $n 
	fi
fi