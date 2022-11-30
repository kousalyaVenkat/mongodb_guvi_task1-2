import pymongo

from pymongo import MongoClient
client = MongoClient()

client = MongoClient('localhost', 27017)

# creating the database

db = client['Telephone']

# creating the collection

collection = db['Telephone_collection']

# creating a document

detail = {
   "Customer Name": "Harish",
   "phone": "9876543223",
   "Category": "Oil & Masala",
   "Sub Category": "Masalas",
   "City": "Vellore",
   "Region": "North",
   "State": "Tamil Nadu"}

# inserting one document to collection

details = db.details
# detail_id = details.insert_one(detail).inserted_id
# print(detail_id)

print(db.list_collection_names())


# inserting many document

new_details = [{"Customer Name": "Jonas",
                "phone": "8767678456",
                "Category": ["Oil & Masala", "Fruits & Veggies"]},
               {"Customer Name": "Hafiz",
                "phone": "7676456788",
                "Sub Category": "Fresh Fruits",
                }]


result = details.insert_many(new_details).inserted_ids
print(result)

# to print all the details of customers using FIND

for d in details.find():
    print(d)

# to find the jonas customer details using FIND

for d in details.find({"Customer Name": "Jonas"}):
    print(d)

print(details.count_documents({}))

print(details.count_documents({"Customer Name": "Harish"}))

# update_one

value = {"$set": {"Sub Category": "Masalas"}}
substitute = {"Customer Name": "Jonas"}
details.update_one(substitute, value)

# delete_one
list1 = list(details.find({"Customer Name": "Jonas"}))
doc = list1[0]
details.delete_one(doc)


print(details.count_documents({}))

print(details.count_documents({"Customer Name": "Jonas"}))