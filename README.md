# MapReduce
Proof of concept - Hadoop - MapReduce


a. Finding top 5 states for one particular year i.e. 2013

Project Description
Finding the top 5 states using telephones for year 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india
Special operations
1.choosing only one particular column 2.eliminating null values
3.converting double number to integer
Goal
To identify which are the top 5 states in which number of telephones used per 100 people is greater and list them in descending order. This is for year 2013.
Jar file
maxTele2013.jar
Java class
MaxTelephonesFinal.java
Input file
/input/Telephones.txt
Output file
/output/Top5Tele



b. Finding top 5 records across the years

Project Description
Finding the top 5 states using highest number of telephones for years through 2000 to 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india
Special operations
1. choosing all columns
2. switch case
3.modifying output using concatenation
Goal
To identify which are the top 5 states in which number of telephones used per 100 people is greater and list them in descending order. This is across all years from 2000 to 2013.
Jar file
highestTele.jar
Java class
HighestTeleAllColmns.java
Input file
/input/Telephones.txt ( same as 1.a)
Output file
/output/highestTeleUse



c. Finding top 25 states across the years with rank

Project Description
Finding the top 25 states using highest number of telephones for years through 2000 to 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india
Special operations
Modifying code for 25 rows
Goal
To identify which are the top 25 states in which number of telephones used per 100 people is greater and list them in descending order. This is across all years from 2000 to 2013.
Jar file
top25Tele.jar
Java class
Top25TeleStatesAcrossYears.java
Input file
/input/Telephones.txt ( same as 1.a)
Output file
/output/top25Tele



d. Finding top 25 states comparing % usage with total use of that particular year

Project Description
Finding the top 25 states with % usage of number of telephones out of total use of that particular year for years through 2000 to 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india

https://data.gov.in/catalog/number-telephones-india-year-wise
Special operation
1.Read 2 datasets
2.Calculate % use of states yearwise ( like JOIN)
Goal
To identify which are the top 25 states in which number of telephones used per 100 people is greater and comparing its use with total usage for that year , list them in descending order. This is across all years from 2000 to 2013.
Jar file
telePercentage.jar
Java class
Top25TeleUsePercentage.java
Input file
/input/Telephones.txt ( same as 1.a)
/input/TotalTelephonesYearly.txt
Output file
/output/telePercentage
