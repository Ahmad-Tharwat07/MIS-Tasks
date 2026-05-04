from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster(['localhost'], port=9042, connection_class=AsyncioConnection)
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS mis_school
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
session.set_keyspace('mis_school')

session.execute("DROP MATERIALIZED VIEW IF EXISTS students_by_grade;")

session.execute("DROP TABLE IF EXISTS students_in_course;")

session.execute("""
    CREATE TABLE students_in_course (
        course_id   text,
        student_id  int,
        name        text,
        grade       int,
        enrolled_at timestamp,
        PRIMARY KEY (course_id, student_id)
    ) WITH CLUSTERING ORDER BY (student_id ASC);
""")
print("Table 'students_in_course' created.")

insert_stmt = session.prepare("""
    INSERT INTO students_in_course (course_id, student_id, name, grade, enrolled_at)
    VALUES (?, ?, ?, ?, ?);
""")

rows = [
    ('CS101', 1, 'Ahmed',  85, datetime(2026, 1, 10)),
    ('CS101', 2, 'Sara',   92, datetime(2026, 1, 11)),
    ('CS101', 3, 'Omar',   78, datetime(2026, 1, 12)),
    ('CS101', 4, 'Mona',   88, datetime(2026, 1, 13)),
    ('CS101', 5, 'Youssef',70, datetime(2026, 1, 14)),
    ('IS202', 1, 'Laila',  95, datetime(2026, 2, 1)),
]
for r in rows:
    session.execute(insert_stmt, r)
print(f"Inserted {len(rows)} rows.")

session.execute("""
    UPDATE students_in_course
    SET grade = 99
    WHERE course_id = 'CS101' AND student_id = 3;
""")
print("Updated student_id=3 in CS101 -> grade=99.")

session.execute("""
    DELETE FROM students_in_course
    WHERE course_id = 'CS101' AND student_id = 5;
""")
print("Deleted student_id=5 from CS101.")

print("\nCurrent rows:")
for row in session.execute("SELECT course_id, student_id, name, grade FROM students_in_course;"):
    print(f"  {row.course_id} | {row.student_id} | {row.name} | {row.grade}")

cluster.shutdown()
print("\nDone.")
