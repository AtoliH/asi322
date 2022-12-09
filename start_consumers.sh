for i in {1..12}
do
  python3 consumer.py & > log_consumer$i.txt 2>&1
  sleep 1
done