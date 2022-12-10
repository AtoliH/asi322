for i in {1..32}
do
    python3 consumer.py $i > log_consumer$i.txt 2>&1 &
done
