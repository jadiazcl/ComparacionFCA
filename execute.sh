#!/bin/sh
echo "Apriori Execute"
echo "---------------"
echo "Mushroom"
python3 Apriori/apriori.py ../Datasets/mushroom.txt 
echo "Flight"
python3 Apriori/apriori.py ../Datasets/flight.txt 
echo "Adult"
python3 Apriori/apriori.py ../Datasets/mushroom.txt 
echo "Car"
python3 Apriori/apriori.py ../Datasets/car.txt 
echo "Credit"
python3 Apriori/apriori.py ../Datasets/credit.txt 
echo "Horse"
python3 Apriori/apriori.py ../Datasets/horse.txt 
echo "Servo"
python3 Apriori/apriori.py ../Datasets/servo_inv.txt 
echo "Credo"
python3 Apriori/apriori.py ../Datasets/credo_cran_cran_context.cxt 
echo "---------------"
echo "Apriori Parallel Execute"
echo "---------------"
echo "Mushroom"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/mushroom.txt 
echo "Flight"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/flight.txt 
echo "Adult"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/mushroom.txt 
echo "Car"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/car.txt 
echo "Credit"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/credit.txt 
echo "Horse"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/horse.txt 
echo "Servo"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/servo_inv.txt 
echo "Credo"
python3 AprioriParalelo/apriori_paralelo.py ../Datasets/credo_cran_cran_context.cxt 
echo "---------------"
echo "NextClosure Execute"
echo "---------------"
echo "Mushroom"
python3 NextClosure/next_closure.py ../Datasets/mushroom.txt 
echo "Flight"
python3 NextClosure/next_closure.py ../Datasets/flight.txt 
echo "Adult"
python3 NextClosure/next_closure.py ../Datasets/mushroom.txt 
echo "Car"
python3 NextClosure/next_closure.py ../Datasets/car.txt 
echo "Credit"
python3 NextClosure/next_closure.py ../Datasets/credit.txt 
echo "Horse"
python3 NextClosure/next_closure.py ../Datasets/horse.txt 
echo "Servo"
python3 NextClosure/next_closure.py ../Datasets/servo_inv.txt 
echo "Credo"
python3 NextClosure/next_closure.py ../Datasets/credo_cran_cran_context.cxt 
echo "---------------"
echo "Paralectico Execute"
echo "---------------"
echo "Mushroom"
python3 Paralectico/paralectico-v1.py ../Datasets/mushroom.txt 
echo "Flight"
python3 Paralectico/paralectico-v1.py ../Datasets/flight.txt 
echo "Adult"
python3 Paralectico/paralectico-v1.py ../Datasets/mushroom.txt 
echo "Car"
python3 Paralectico/paralectico-v1.py ../Datasets/car.txt 
echo "Credit"
python3 Paralectico/paralectico-v1.py ../Datasets/credit.txt 
echo "Horse"
python3 Paralectico/paralectico-v1.py ../Datasets/horse.txt 
echo "Servo"
python3 Paralectico/paralectico-v1.py ../Datasets/servo_inv.txt 
echo "Credo"
python3 Paralectico/paralectico-v1.py ../Datasets/credo_cran_cran_context.cxt 