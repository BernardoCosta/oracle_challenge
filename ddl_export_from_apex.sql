  /*CREATE TABLE "HTMLDB_PLAN_TABLE" 
   (	"STATEMENT_ID" VARCHAR2(30), 
	"PLAN_ID" NUMBER, 
	"TIMESTAMP" DATE, 
	"REMARKS" VARCHAR2(4000), 
	"OPERATION" VARCHAR2(30), 
	"OPTIONS" VARCHAR2(255), 
	"OBJECT_NODE" VARCHAR2(128), 
	"OBJECT_OWNER" VARCHAR2(128), 
	"OBJECT_NAME" VARCHAR2(128), 
	"OBJECT_ALIAS" VARCHAR2(261), 
	"OBJECT_INSTANCE" NUMBER(*,0), 
	"OBJECT_TYPE" VARCHAR2(128), 
	"OPTIMIZER" VARCHAR2(255), 
	"SEARCH_COLUMNS" NUMBER, 
	"ID" NUMBER(*,0), 
	"PARENT_ID" NUMBER(*,0), 
	"DEPTH" NUMBER(*,0), 
	"POSITION" NUMBER(*,0), 
	"COST" NUMBER(*,0), 
	"CARDINALITY" NUMBER(*,0), 
	"BYTES" NUMBER(*,0), 
	"OTHER_TAG" VARCHAR2(255), 
	"PARTITION_START" VARCHAR2(255), 
	"PARTITION_STOP" VARCHAR2(255), 
	"PARTITION_ID" NUMBER(*,0), 
	"OTHER" LONG, 
	"DISTRIBUTION" VARCHAR2(30), 
	"CPU_COST" NUMBER(*,0), 
	"IO_COST" NUMBER(*,0), 
	"TEMP_SPACE" NUMBER(*,0), 
	"ACCESS_PREDICATES" VARCHAR2(4000), 
	"FILTER_PREDICATES" VARCHAR2(4000), 
	"PROJECTION" VARCHAR2(4000), 
	"TIME" NUMBER(*,0), 
	"QBLOCK_NAME" VARCHAR2(128)
   ) ; */

  CREATE TABLE "ITEM" 
   (	"ITEM" VARCHAR2(25) NOT NULL ENABLE, 
	"DEPT" NUMBER(4,0) NOT NULL ENABLE, 
	"ITEM_DESC" VARCHAR2(25) NOT NULL ENABLE, 
	 CONSTRAINT "PK_ITEM" PRIMARY KEY ("ITEM")
  USING INDEX  ENABLE
   ) ;

  CREATE TABLE "ITEM_LOC_SOH" 
   (	"ITEM" VARCHAR2(25) NOT NULL ENABLE, 
	"LOC" NUMBER(10,0) NOT NULL ENABLE, 
	"DEPT" NUMBER(4,0) NOT NULL ENABLE, 
	"UNIT_COST" NUMBER(20,4) NOT NULL ENABLE, 
	"STOCK_ON_HAND" NUMBER(12,4) NOT NULL ENABLE, 
	 CONSTRAINT "PK_ITEM_LOC_SOH" PRIMARY KEY ("ITEM", "LOC")
  USING INDEX  ENABLE
   ) ;

  CREATE TABLE "ITEM_LOC_SOH_HIST" 
   (	"ITEM" VARCHAR2(25) NOT NULL ENABLE, 
	"LOC" NUMBER(10,0) NOT NULL ENABLE, 
	"DEPT" NUMBER(4,0) NOT NULL ENABLE, 
	"UNIT_COST" NUMBER(20,4) NOT NULL ENABLE, 
	"STOCK_ON_HAND" NUMBER(12,4) NOT NULL ENABLE, 
	"STOCK_VALUE" NUMBER(20,4) NOT NULL ENABLE, 
	"LOAD_DATE" TIMESTAMP (6) NOT NULL ENABLE, 
	 CONSTRAINT "PK_ITEM_LOC_SOH_HIST" PRIMARY KEY ("ITEM", "LOC")
  USING INDEX  ENABLE
   ) ;

  CREATE TABLE "LOC" 
   (	"LOC" NUMBER(10,0) NOT NULL ENABLE, 
	"LOC_DESC" VARCHAR2(25) NOT NULL ENABLE, 
	 CONSTRAINT "PK_LOC" PRIMARY KEY ("LOC")
  USING INDEX  ENABLE
   ) ;

  CREATE TABLE "USERS" 
   (	"ID" VARCHAR2(50) NOT NULL ENABLE, 
	"NAME" VARCHAR2(50) NOT NULL ENABLE, 
	 CONSTRAINT "PK_USERS" PRIMARY KEY ("ID")
  USING INDEX  ENABLE
   ) ;

  CREATE TABLE "USER_DEPT" 
   (	"USER_ID" VARCHAR2(50) NOT NULL ENABLE, 
	"DEPT" NUMBER(4,0) NOT NULL ENABLE, 
	 CONSTRAINT "PK_USER_DEPT" PRIMARY KEY ("USER_ID", "DEPT")
  USING INDEX  ENABLE
   ) ;

  ALTER TABLE "ITEM_LOC_SOH" ADD CONSTRAINT "FK_ITEM_LOC_SOH_ITEM" FOREIGN KEY ("ITEM")
	  REFERENCES "ITEM" ("ITEM") ENABLE;
  ALTER TABLE "ITEM_LOC_SOH" ADD CONSTRAINT "FK_ITEM_LOC_SOH_LOC" FOREIGN KEY ("LOC")
	  REFERENCES "LOC" ("LOC") ENABLE;

  CREATE INDEX "ITEM_LOC_SOH_IX" ON "ITEM_LOC_SOH" ("LOC", "DEPT") 
  ;

  ALTER TABLE "ITEM_LOC_SOH_HIST" ADD CONSTRAINT "FK_ITEM_LOC_SOH_ITEM_HIST" FOREIGN KEY ("ITEM")
	  REFERENCES "ITEM" ("ITEM") ENABLE;
  ALTER TABLE "ITEM_LOC_SOH_HIST" ADD CONSTRAINT "FK_ITEM_LOC_SOH_LOC_HIST" FOREIGN KEY ("LOC")
	  REFERENCES "LOC" ("LOC") ENABLE;

  ALTER TABLE "USER_DEPT" ADD CONSTRAINT "FK_USER_ID" FOREIGN KEY ("USER_ID")
	  REFERENCES "USERS" ("ID") ENABLE;


  CREATE INDEX "ITEM_LOC_SOH_IX" ON "ITEM_LOC_SOH" ("LOC", "DEPT") 
  ;

  CREATE UNIQUE INDEX "PK_ITEM" ON "ITEM" ("ITEM") 
  ;

  CREATE UNIQUE INDEX "PK_ITEM_LOC_SOH" ON "ITEM_LOC_SOH" ("ITEM", "LOC") 
  ;

  CREATE UNIQUE INDEX "PK_ITEM_LOC_SOH_HIST" ON "ITEM_LOC_SOH_HIST" ("ITEM", "LOC") 
  ;

  CREATE UNIQUE INDEX "PK_LOC" ON "LOC" ("LOC") 
  ;

  CREATE UNIQUE INDEX "PK_USERS" ON "USERS" ("ID") 
  ;

  CREATE UNIQUE INDEX "PK_USER_DEPT" ON "USER_DEPT" ("USER_ID", "DEPT") 
  ;


create or replace package "LOAD_HISTORIC" as

--Loads for all locations
procedure process_item_loc_soh(p_status out varchar2,
                               p_error out varchar2);

--Loads for specified location
procedure process_item_loc_soh(p_loc in loc.loc%type,
                               p_status out varchar2,
                               p_error out varchar2);                               

--Loads for all locations using merge
procedure process_item_loc_soh_merge(p_status out varchar2,
                                     p_error out varchar2);

--Loads for specified location using merge
procedure process_item_loc_soh_merge(p_loc in loc.loc%type,
                                     p_status out varchar2,
                                     p_error out varchar2);       

end "LOAD_HISTORIC";
/
create or replace package "PIPELINE" as

  function get_locs return locs_type_tbl pipelined;


end "PIPELINE";
/

create or replace type locs_type as object (
  loc number,
  loc_desc varchar2(50)
)
/
create or replace type locs_type_tbl as table of locs_type
/

  CREATE UNIQUE INDEX "PK_ITEM" ON "ITEM" ("ITEM") 
  ;

  CREATE INDEX "ITEM_LOC_SOH_IX" ON "ITEM_LOC_SOH" ("LOC", "DEPT") 
  ;
  CREATE UNIQUE INDEX "PK_ITEM_LOC_SOH" ON "ITEM_LOC_SOH" ("ITEM", "LOC") 
  ;

  CREATE UNIQUE INDEX "PK_ITEM_LOC_SOH_HIST" ON "ITEM_LOC_SOH_HIST" ("ITEM", "LOC") 
  ;

  CREATE UNIQUE INDEX "PK_LOC" ON "LOC" ("LOC") 
  ;

  CREATE UNIQUE INDEX "PK_USERS" ON "USERS" ("ID") 
  ;

  CREATE UNIQUE INDEX "PK_USER_DEPT" ON "USER_DEPT" ("USER_ID", "DEPT") 
  ;



create or replace package body "LOAD_HISTORIC" as

procedure process_item_loc_soh(p_status out varchar2,
                               p_error out varchar2)

is
   
begin

    INSERT INTO item_loc_soh_hist (item, loc, dept, unit_cost, stock_on_hand, stock_value, load_date)  
    SELECT item, 
           loc, 
           dept, 
           unit_cost, 
           stock_on_hand, 
           unit_cost * stock_on_hand, 
           sysdate() 
    FROM item_loc_soh;
    
    p_status := 'S';

exception
 WHEN OTHERS THEN
    p_status := 'E';    
    p_error := 'Error - SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM;
    
end process_item_loc_soh;

procedure process_item_loc_soh(p_loc in loc.loc%type,
                               p_status out varchar2,
                               p_error out varchar2)

is

begin

    INSERT INTO item_loc_soh_hist (item, loc, dept, unit_cost, stock_on_hand, stock_value, load_date)
    SELECT item, 
           loc, 
           dept, 
           unit_cost, 
           stock_on_hand, 
           unit_cost * stock_on_hand, 
           sysdate() 
    FROM item_loc_soh
    WHERE loc = p_loc;
    
    p_status := 'S';

exception
 WHEN OTHERS THEN
    p_status := 'E';    
    p_error := 'Error - SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM;
    
end process_item_loc_soh;

procedure process_item_loc_soh_merge(p_status out varchar2,
                                     p_error out varchar2)

is
   
begin

    merge into item_loc_soh_hist ih
    using item_loc_soh i
    on (ih.item = i.item
    and ih.loc = i.loc)
  when matched then
    update set 
    ih.dept = i.dept,
    ih.unit_cost = i.unit_cost,
    ih.stock_on_hand = i.stock_on_hand,
    ih.stock_value = i.unit_cost * i.stock_on_hand,
    load_date = sysdate()
  when not matched then
    insert (item,
            loc,
            dept,
            unit_cost,
            stock_on_hand,
            stock_value,
            load_date)
    values (i.item,
            i.loc,
            i.dept,
            i.unit_cost,
            i.stock_on_hand,
            i.unit_cost * i.stock_on_hand,
            sysdate());    

    p_status := 'S';

exception
 WHEN OTHERS THEN
    p_status := 'E';    
    p_error := 'Error - SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM;
    
end process_item_loc_soh_merge;

procedure process_item_loc_soh_merge(p_loc in loc.loc%type,
                                     p_status out varchar2,
                                     p_error out varchar2)

is

begin

    merge into item_loc_soh_hist ih
    using item_loc_soh i
    on (i.loc = p_loc
    and ih.item = i.item
    and ih.loc = i.loc)
  when matched then
    update set 
    ih.dept = i.dept,
    ih.unit_cost = i.unit_cost,
    ih.stock_on_hand = i.stock_on_hand,
    ih.stock_value = i.unit_cost * i.stock_on_hand,
    load_date = sysdate()
  when not matched then
    insert (item,
            loc,
            dept,
            unit_cost,
            stock_on_hand,
            stock_value,
            load_date)
    values (i.item,
            i.loc,
            i.dept,
            i.unit_cost,
            i.stock_on_hand,
            i.unit_cost * i.stock_on_hand,
            sysdate());    

exception
 WHEN OTHERS THEN
    p_status := 'E';    
    p_error := 'Error - SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM;
    
end process_item_loc_soh_merge;

end "LOAD_HISTORIC";
/
create or replace package body "PIPELINE" as

 function get_locs return locs_type_tbl pipelined
 is

 l_row locs_type := locs_type(null,null);

 cursor c_get_locs is
 select loc_desc as loc_desc,
        loc from loc;

 begin

    for r_get_locs in c_get_locs loop

        l_row.loc := r_get_locs.loc;
        l_row.loc_desc := r_get_locs.loc_desc;

        pipe row (l_row);

    end loop;

    return;

 end get_locs;       

end "PIPELINE";
/
