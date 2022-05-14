# energy_data_pull
This repository contains 
- Two Python scripts that can pull data from an API and store it to an AWS RDS database
- Two Tableau workbooks that used the data

The Tableau dashboards can also be accessed Tableau Public:

Energy Sites Snapshot by Minute: https://public.tableau.com/app/profile/kai.sun3223/viz/MinuteSnapshotDashboard/MinuteSnapshotDashboard?publish=yes 
Energy Sites Historic Overview / Energy Sites Anomalies: https://public.tableau.com/app/profile/kai.sun3223/viz/EnergySitesHistoricTrendandAnomolyDetection/HistoricOverview

For this task, the current process is: 
1) Run script locally to pull real-time API data
2) Store data in a pandas DataFrame
3) Access database
4) Update/add data to tables in database
5) Stop the running process for a desired time interval (1 minute and 10 minutes respectively) and re-start from 1) 

Although this process does not require too much local memory, it does require a local machine that runs continuously so that the database stays updated, which is not ideal. The storage of the database is also very limited. Ideally, I would move the process to the cloud or a distributed system, where the database can be updated continuously and reliably. If relational databases are not enough to handle the volume of data, NoSQL databases options like MongoDB or Neo4j should be explored 

If you have any questions regarding this repo, feel free to reach me at: Kai.Sun.2021@marshall.usc.edu


