/*
Preparation
*/

/*Eg: (499152, 72690, '2000-07-20', '2001-07-20')*/
drop table if exists salaries;
create table if not exists salaries 
  (emp_no bigint, salary bigint, from_date date, to_date date) 
  stored as textfile;

/*Eg: (499993, '1963-06-04', 'DeForest', 'Mullainathan', 'M', '1997-04-07')*/
drop table if exists employees;
create table if not exists employees
  (emp_no bigint, birth_date date, first_name string, last_name string, 
   gender string, hire_date date) 
  stored as textfile;

/*
for E task:
The ‘to_date’ column overlaps with the ‘from_date’ column in the ‘salaries’ 
table.  This results in the employee having two salary records on the day of 
the ‘to_date’ column. Fix it by decrementing the ‘to_date’ column by one 
day (e.g. from ‘1987-06-18’ to ‘1987-06-17’) to make each salary 
record exclusive.
*/
drop table if exists salaries_new;
create table if not exists salaries_new 
  (emp_no bigint, salary bigint, from_date date, to_date date) 
  stored as textfile;
insert into table salaries_new 
  select S.emp_no, S.salary, S.from_date, date_sub(to_date, 1) 
  from salaries S;

/*
for F task:
The first salary record for an employee should reflect the day they joined the 
company.  However, the ‘employees.hire_date’ column doesn’t always reflect 
this.  Clean the data by replacing the ‘employees.hire_date’ column with the 
first salary record for an employee.
*/
drop table if exists employees_new;
create table if not exists employees_new
  (emp_no bigint, birth_date date, first_name string, last_name string, 
   gender string, hire_date date) 
  stored as textfile;
insert into table employees_new
  select E.emp_no, E.birth_date, E.first_name, 
         E.last_name, E.gender, NS.first_day 
  from employees E
  left outer join (
    select S.emp_no, min(S.from_date) as first_day from salaries_new S 
    group by S.emp_no) NS
  on E.emp_no == NS.emp_no;

/*
for G task:
Determine which employee lasted less than two weeks in the job in May 1985?
*/
select E.emp_no, E.first_name, E.last_name from employees_new E 
join (
select emp_no, max(to_date) as final_day from salaries_new 
  group by emp_no 
  having datediff(max(to_date), "1985-05-01")<14 
  and datediff(max(to_date), "1985-05-01")>0 
) T on E.emp_no=T.emp_no;
/*
Result:
92539   Hauke   Bouloucos
464017  Zine    Kusakari
*/



