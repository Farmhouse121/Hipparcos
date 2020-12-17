drop function if exists DegreesFromHMS;
create function DegreesFromHMS(hms varchar(15)) returns double
    deterministic
    comment 'Converts H:M:S to degrees'
    return substring_index(hms,' ',1)*360e0/24e0+substring_index(substring_index(hms,' ',2),' ',-1)/60e0+substring_index(hms,' ',-1)/3600e0;
drop function if exists DegreesFromDMS;
create function DegreesFromDMS(dms varchar(15)) returns double
    deterministic
    comment 'Converts D:M:S to degrees'
    return substring_index(dms,' ',1)+substring_index(substring_index(dms,' ',2),' ',-1)/60e0+substring_index(dms,' ',-1)/3600e0;
select
    RAhms,DEdms,DegreesFromHMS(RAhms),DegreesFromDMS(DEdms)
from
    Hipparcos
where
    RAdeg is null
or  DEdeg is null;

update Hipparcos set RAdeg=DegreesFromHMS(RAhms) where RAdeg is null;
update Hipparcos set DEdeg=DegreesFromDMS(DEdms) where DEdeg is null;

alter table Hipparcos
    add Coordinates geometry as (st_geomfromtext(concat('POINT(',case when RAdeg>180e0 then RAdeg-360e0 else RAdeg end,' ',DEdeg,')'),4326,'axis-order=long-lat')) stored srid 4326 not null comment 'Coordinates in WGS84 as there is no other way to store it.',
    add spatial key Coordinates_key (Coordinates);

alter table Hipparcos add Distance double as (case when Plx>0e0 then 1000e0/Plx end) stored comment 'Distance in parsecs computed from trigonometric paralax in milliarcseconds.';
