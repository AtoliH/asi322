for i in {1..12}
do
  python3 consumer.py &> log_consumer$i.txt &
  sleep 1
done