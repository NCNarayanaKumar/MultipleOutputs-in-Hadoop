# MultipleOutputs-in-Hadoop

There may be cases where we need to partition our data based on certion condition.
Say for example, Consider this Employee data

EmpId,EmpName,Age,Gender,Salary
1201,gopal,45,Male,50000
1202,manisha,40,Female,51000
1203,khaleel,34,Male,30000
1204,prasanth,30,Male,31000
1205,kiran,20,Male,40000
1206,laxmi,25,Female,35000
1207,bhavya,20,Female,15000
1208,reshma,19,Female,14000
1209,kranthi,22,Male,22000
1210,Satish,24,Male,25000
1211,Krishna,25,Male,26000
1212,Arshad,28,Male,20000
1213,lavanya,18,Female,8000

Lets assume one condition.
We need to seperate above data based on Gender (there can be more scenarios)

Expected outcome will be like this 

Female

1213,lavanya,18,Female,8000
1202,manisha,40,Female,51000
1206,laxmi,25,Female,35000
1207,bhavya,20,Female,15000
1208,reshma,19,Female,14000
Male

1211,Krishna,25,Male,26000
1212,Arshad,28,Male,20000
1201,gopal,45,Male,50000
1209,kranthi,22,Male,22000
1210,Satish,24,Male,25000
1203,khaleel,34,Male,30000
1204,prasanth,30,Male,31000
1205,kiran,20,Male,40000

Details can be found here : http://unmeshasreeveni.blogspot.in/2015/12/partitioning-data-using-hadoop.html
Happy Hadooping..............
