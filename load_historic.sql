
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