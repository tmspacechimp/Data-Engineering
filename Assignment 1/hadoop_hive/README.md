# docker-hadoop-hive-parquet

This project will showcase how to spin up a Hadoop cluster with Hive in order to run SQL queries on Parquet files. Images for the nodes are based on https://hub.docker.com/u/bde2020 base images.

All of this makes more sense if you follow the link in the repository to the article on Medium :)


--- 

**pyspark-notebook** კონტეინერში გვაქვს PySpark და Jupyter. UI არის ამ მისამართზე: **http://localhost:8080/** მოითხოვს პაროლს ან ტოკენს. ტოკენის გასაგებად კონტეინერის კონსოლში უნდა გაუშვათ **jupyter notebook list**. აქიდან შეგიძლიათ პირდაპირ ლინკი დააკოპიროთ, localhost:8080 ჩაუწეროთ დასაწყისში 0.0.0.0:8888-ის ნაცვლად და ისე გაიაროთ ავტორიზაცია, ან ტოკენი გადმოაკოპიროთ და ტოკენით შეხვიდეთ, ან ტოკენი შეცვალოთ სასურველი password-ით. 

ამ ნოუთბუქში pip დაგხვდებათ და შეგიძლიათ დააყენოთ სასურველი ბიბლიოთეკები.

