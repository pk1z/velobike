#!/bin/bash
_now=$(date +"%s_%T")
_file="/root/velobike_$_now.json"
echo "Saving json to $_file..."
wget http://velobike.ru/proxy/parkings/ -O "$_file"
