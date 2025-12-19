#!/bin/bash

sudo bash -c 'echo "export STORAGE_PREFIX=s3://aperezpe2018-meteo-study-bucket" >> /etc/profile.d/custom_env.sh'
sudo bash -c 'echo "export STORAGE_BASE=/data" >> /etc/profile.d/custom_env.sh'
sudo chmod +x /etc/profile.d/custom_env.sh