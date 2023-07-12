create or replace package "PIPELINE" as

  function get_locs return locs_type_tbl pipelined;


end "PIPELINE";
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
