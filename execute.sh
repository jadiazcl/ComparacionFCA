#!/bin/sh
python3 apriori_ray_resurrection.py ../data/mushroom.txt 
echo "1"
python3 apriori_ray_resurrection.py ../data/flight.txt 
echo "2"
python3 raypy/ray_miner.v7.py ../data/mushroom.txt 8
echo "3"
python3 raypy/ray_miner.v7.py ../data/mushroom.txt 25
echo "3.1"
python3 raypy/ray_miner.v7.py ../data/mushroom.txt 20
echo "4"
python3 raypy/ray_miner.v7.py ../data/mushroom.txt 30
echo "5"
python3 raypy/ray_miner.v7.py ../data/flight.txt 8
echo "6"
python3 raypy/ray_miner.v7.py ../data/flight.txt 20
echo "8"
python3 raypy/ray_miner.v7.py ../data/flight.txt 25
echo "9"
python3 raypy/ray_miner.v7.py ../data/flight.txt 30
