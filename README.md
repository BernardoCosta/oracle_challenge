-- Oracle Challenge - Bernardo Costa

--Note:
--Instruction's insert queries were slightly altered to contain less data due to tablespace constraints on apex workspace.

insert into item(item,dept,item_desc)
select level, round(DBMS_RANDOM.value(1,100)), translate(dbms_random.string('a', 20), 'abcXYZ', level) from dual connect by level <= 1000;

insert into loc(loc,loc_desc)
select level+100, translate(dbms_random.string('a', 20), 'abcXYZ', level) from dual connect by level <= 100;


1)
-- Set item and loc as PK as each entry corresponds to a unique item/loc combination
alter table item_loc_soh
add constraint item_loc_soh_pk primary key (item, loc);

-- Add foreign keys to item_loc_soh
alter table item_loc_soh
add constraint fk_item_loc_soh_item
  foreign key (item)
  references item(item);
  
alter table item_loc_soh
add constraint fk_item_loc_soh_loc
  foreign key (loc)
  references loc(loc);  

--Set PK for item and loc tables
alter table item
add constraint pk_item
primary key(item);

alter table loc
add constraint pk_loc
primary key(loc);


-- Create index on dept to improve performance when users filter by loc and then dept
create index item_loc_soh_ix
  on item_loc_soh (loc, dept);
  
2)
-- I have rewritten the table definition using partitioning:
create table item_loc_soh
(item varchar2(25) not null,
loc number(10) not null,
dept number(4) not null,
unit_cost number(20,4) not null,
stock_on_hand number(12,4) not null,
constraint pk_item_loc_soh
primary key(item, loc),
constraint fk_item_loc_soh_item
foreign key(item)
references item(item),
constraint fk_item_loc_soh_loc
foreign key(loc)
references loc(loc)
)
partition by range (loc)
subpartition by hash (dept)
subpartitions 7
(partition item_loc_soh_p1 values less than (116),
 partition item_loc_soh_p2 values less than (131),
 partition item_loc_soh_p3 values less than (146),
 partition item_loc_soh_p4 values less than (161),
 partition item_loc_soh_p5 values less than (176),
 partition item_loc_soh_p6 values less than (191),
 partition item_loc_soh_p7 values less than (maxvalue)
 );

-- This solution separates the data into different partitions and subpartitions. 
-- Partitioning is defined on the loc level as that is the first query parameter set by the user.
-- It is defined by a range, considering that the data has a minimum and maximum value of 101 and 200.
-- item_loc_soh_p7 contains less values than the other partitions to accommodate for some further store/warehouse expansion. However, if many more locs are added, the partitions should be rethinked.
-- Subpartitioning is defined on the dept level as this is also a highly used column to filter data.
-- Hash subpartitioning is the only choice because dept is not part of the key of the table.
-- The number of partitions/subpartitions would require further testing in order to maximize performance. Might even be that the best approach would be to use a partition for each loc, and not use subpartitioning altogether.
-- IMPORTANT NOTE: Although this is the solution that I would like to test, due to the tablespace constraints faced, this solution is unviable as it takes too much storage space and I was unable to add a meaningful amount of data to the table.
-- As such, the solution currently utilizes the table without partitioning as follows:
create table item_loc_soh
(item varchar2(25) not null,
loc number(10) not null,
dept number(4) not null,
unit_cost number(20,4) not null,
stock_on_hand number(12,4) not null,
constraint pk_item_loc_soh
primary key(item, loc),
constraint fk_item_loc_soh_item
foreign key(item)
references item(item),
constraint fk_item_loc_soh_loc
foreign key(loc)
references loc(loc)
);


3)
-- I don't believe there would be row contention as this specific application only queries and displays data. No modifications are being made to item_loc_soh, hence no row locking should occur.

4)
-- Unfortunately I found the question a bit too vague. I didn't understand where the view was supposed to be used, nor what was meant by 'required fields' as every field is not nullable.

5)
-- create user table and insert user
create table users(
    id varchar2(50) not null,
    name varchar2(50) not null,
    constraint pk_users
    primary key(id)
);

insert into users values ('bernardopintocosta@gmail.com','Bernardo Costa');

-- create table for user/dept association and insert values
create table user_dept(
    user_id varchar2(50) not null,
    dept number(4) not null,
    constraint pk_user_dept
    primary key(user_id, dept),
    constraint fk_user_id
    foreign key(user_id)
    references users(id)
);

insert into user_dept values ('bernardopintocosta@gmail.com','14');
insert into user_dept values ('bernardopintocosta@gmail.com','27');
insert into user_dept values ('bernardopintocosta@gmail.com','50');

6)
-- I had originally interpreted this exercise as in updating the records in the historic table, or adding the recently created records. For this purpose I had used a merge statement.
-- Then I realised if it would work like that, then this table wouldn't be really useful as an historic table. So I redesigned the package to simply add new records.
-- Still, as of now I'm unsure I interpreted this exercise correctly, since simply inserting records will result in absurd amounts of data. 
-- So I'm keeping both versions of the procedures in the package. The procedures are called process_item_loc_soh (insert) and process_item_loc_soh_merge (merge) in the package LOAD_HISTORIC.

-- Create historic table. Also added a load_date column to keep track of when the record was added, since it's a historic table. 
-- I'm keeping the primary key here, so this version of the table doesn't work with the insert procedure until the PK is disabled.
create table item_loc_soh_hist
(item varchar2(25) not null,
loc number(10) not null,
dept number(4) not null,
unit_cost number(20,4) not null,
stock_on_hand number(12,4) not null,
stock_value number(20,4) not null,
load_date timestamp not null,
constraint pk_item_loc_soh_hist
primary key(item, loc),
constraint fk_item_loc_soh_item_hist
foreign key(item)
references item(item),
constraint fk_item_loc_soh_loc_hist
foreign key(loc)
references loc(loc)
);



7)
-- Changed the query from apex item 'Items' in the page designer to the following:
select i.ITEM,
       i.LOC,
       i.DEPT,
       i.UNIT_COST,
       i.STOCK_ON_HAND
  from ITEM_LOC_SOH i
 left join USER_DEPT u
 on i.dept = u.dept
 where i.loc = :P1_LOC
 and u.user_id = lower(:APP_USER)

-- Also, for some reason, the page stopped working. I had to add 'to_char' to the query below, which is used in the 'P1_LOC' item:
 select loc || ' - ' || loc_desc, to_char(loc) from loc
  
8)
-- Create record type and table type to use in pipeline package
  create or replace type locs_type as object (
  loc number,
  loc_desc varchar2(50)
);

create or replace type locs_type_tbl as table of locs_type;

-- The package created for this exercise is PIPELINE, function get_locs
-- Altered the query for 'P1_LOC' item again to call the function as a table:
select loc || ' - ' || loc_desc, to_char(loc) from table(pipeline.get_locs)

9)
-- Looking at the explain plan, there is no system in place to optimize performance.
-- Taking by example my dataset (taking into account that it is smaller due to aforementioned constaints), let's look at the following query:
select * from item_loc_soh where loc = 119 and dept = 97;
-- By dropping the index on item_loc_soh, the explain plan shows that the CPU cost for this operation is 137.
-- However, when re-adding the index to the table, the cost drops down to only 10.

10)
-- Unfortunately, unable to test properly due to small sample size. 
-- For my dataset, 100.000 records, the insert operation is taking on average 0.4 seconds, and the merge operation averaging 0.65 seconds.

11)
-- I haven't been exposed to this type of analysis before and would need more time to both learn how it works and then analyze the problem and suggest a solution.

12)
-- Not able to attempt this challenge due to running out of time.
