import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["AssignmentDB"]

students = db["Students"]
courses = db["Courses"]

students.drop()
courses.drop()

students_data = [
    {"_id": 1, "name": "Ahmed", "age": 20},
    {"_id": 2, "name": "Sara", "age": 22},
    {"_id": 3, "name": "Ali", "age": 21}
]
courses_data = [
    {"_id": 101, "title": "Database", "hours": 3},
    {"_id": 102, "title": "Programming", "hours": 4},
    {"_id": 103, "title": "Networks", "hours": 2}
]

students.insert_many(students_data)
courses.insert_many(courses_data)
print("Step 1: Collections and Documents created.")

students.delete_one({"name": "Ali"})
courses.delete_one({"_id": 103})
print("Step 2: One document deleted from each collection.")

students.update_many({}, {"$set": {"Score": [10, 10, 10, 10]}})
print("Step 3: 'Score' array added to all documents.")

students.update_one({"_id": 1}, {"$set": {"Score.2": 5}})

students.update_many({"_id": {"$ne": 1}}, {"$set": {"Score.3": 6}})
print("Step 4: Scores updated based on _id logic.")

students.update_many(
    {}, 
    {"$mul": {"Score.$[]": 20}}
)
print("Step 5: All elements in 'Score' multiplied by 20.")

print("\n--- Process Completed Successfully! ---")


students.update_one(
    {"_id": 1}, 
    {"$set": {"enrolled_courses": [101, 102]}}
)
print("Part 2: Relationship created (Student 1 linked to courses 101, 102).")

# db.Students.aggregate([
#   { $match: { _id: 1 } },
#   {
#     $lookup: {
#       from: "Courses",
#       localField: "enrolled_courses",
#       foreignField: "_id",
#       as: "course_details"
#     }
#   }
# ])