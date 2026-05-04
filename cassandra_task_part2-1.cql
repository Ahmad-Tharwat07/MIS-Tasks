USE mis_school;

SELECT course_id, student_id, name, grade
FROM   students_in_course
WHERE  course_id = 'CS101'
ORDER  BY student_id DESC;

CREATE MATERIALIZED VIEW IF NOT EXISTS students_by_grade AS
    SELECT grade, course_id, student_id, name
    FROM   students_in_course
    WHERE  grade      IS NOT NULL
      AND  course_id  IS NOT NULL
      AND  student_id IS NOT NULL
    PRIMARY KEY (grade, course_id, student_id);

SELECT * FROM students_by_grade WHERE grade = 99;
