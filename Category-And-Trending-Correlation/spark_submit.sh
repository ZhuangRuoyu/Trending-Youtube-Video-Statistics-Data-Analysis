#!/bin/bash

spark-submit  \
   --master local[2] \
   TrendingComparison.py \
   --input hdfs://localhost:9000/user/rzhu9225/ \
   --output SparkOutputQ1 \
   --countryA GB \
   --countryB US
