#!/bin/bash

while true
do
    /opt/prom_scraper/prom_scraper.py -c /opt/prom_scraper/prom_config.yml
    sleep 10
done

exit 0
