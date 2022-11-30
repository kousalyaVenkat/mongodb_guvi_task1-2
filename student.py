import pymongo

from pymongo import MongoClient

client = MongoClient()

client = MongoClient('localhost', 27017)

# creating the database

db = client['student_details']

# creating the collection

collection = db['exam_scores']

print(db.list_collection_names())

print(collection.count_documents({}))

# agg = db.exam_scores.aggregate([{"$group": {"_id": "$scores.type.exam", "count": {"$max": "$scores.score"}}}])
# for i in agg:
#     print(i)

s1 = {"$unwind": "$scores"}
s2 = {"$group": {"_id": "$_id", "name": {"$addToSet": '$name'}, "total_score": {"$sum": "$scores.score"}}}
s3 = {"$sort": {"total_score": -1}}
s6 = {"$limit": 1}
for i in collection.aggregate([s1, s2, s3, s6]):
    print(i)

# 2
h1 = {"$unwind": "$scores"}
h2 = {"$match": {"$and": [{"scores.type": {"$in": ["exam"]}}, {"scores.score": {"$gt": 40}},
                          {"scores.score": {"$lte": 52.3}}]}}

for i in collection.aggregate([h1, h2]):
    print(i)


# 3
def aggr(std):
    total_percentage = collection.aggregate([{'$unwind': '$scores'},
                                       {'$group': {"_id": "$_id", "name": {"$first": "$name"},
                                                   "max_score": {'$sum': "$scores.score"}}},
                                       {'$addFields': {"total_percentage": {'$divide': ["$max_score", 3]}}},
                                       {'$addFields': {"pass/fail": {
                                           "$cond": {'if': {"$gte": ["$total_percentage", 40]}, 'then': "Pass",
                                                     'else': "Fail"}}}}])
    return total_percentage


total_percentage = aggr(collection)
for i in total_percentage:
    if (i['total_percentage'] >= 40) and (i['total_percentage'] <= 55):
        print(i)
total_percentage = aggr(collection)
for i in total_percentage:
    print(i)

# 4)Find the total and average of the exam, quiz and homework and store them in a separate collection.
db.create_collection('total_average_students')
x2 = collection.aggregate([{'$unwind': '$scores'}, {'$group': {'_id': {'type': "$scores.type"},
                                                               "total_marks:": {'$sum': "$scores.score"},
                                                               "average_score:": {'$avg': "$scores.score"}}}])
t_a = db.total_average_students
for i in x2:
    print(i)
    t_a.insert_one(i)





# 5)Create a new collection which consists of students who scored below average and above 40% in all the categories.
total_percentage = aggr(collection)
db.create_collection('students_passed_below_average')  # commented once created
p_a = db.students_passed_below_average

for i in total_percentage:
    if (i['total_percentage'] > 40) and (i['total_percentage']) <= 55:
        print(i)
        p_a.insert_one(i)











# 6)Create a new collection which consists of students who scored below the fail mark in all the categories.
failure = aggr(collection)
# db.create_collection('students_failed')  # collection is created once hence commented
fail = db.students_failed
for i in failure:
    if i['pass/fail'] == 'Fail':
        print(i)
        fail.insert_one(i)



# 7 Create a new collection consists of students who scored above pass mark
total_percentage = aggr(collection)
db.create_collection('passed_students')
p = db.passed_students
for i in total_percentage:
    if i['total_percentage'] >= 40:
        print(i)
        p.insert_one(i)
