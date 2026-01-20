import csv

data = [
    [101,"Amit","Electronics",2,30000],
    [102,"Priya","Clothing",3,2000],
    [103,"Rahul","Electronics",1,15000],
    [104,"Neha","Clothing",5,1800],
    [105,"Karan","Furniture",1,45000],
    [106,"Amit","Electronics",3,28000]
]
 
with open("order.csv", 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Id","Name","Category","Quantity","Price"])
    writer.writerows(data)

print("order.csv file created successfully.")