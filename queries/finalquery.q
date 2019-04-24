with q_source as (select *
  from #
  where season = 3
),

q_device_0 as (select
  cityid,
  device,
  count(*) as cntd
from q_source lateral view parseUA(user_agent) uap as device, os, browser
group by cityid, device),

q_device_1 as (select cityid, max(cntd) as maxd
from q_device_0
group by cityid),

q_device_2 as (select q_device_1.cityid as cityid, device, maxd
from q_device_1 inner join q_device_0 on q_device_1.cityid = q_device_0.cityid and q_device_1.maxd = q_device_0.cntd),

q_os_0 as (select
  cityid,
  os,
  count(*) as cntos
from q_source lateral view ua8(user_agent) uap as device, os, browser
group by cityid, os),

q_os_1 as (select cityid, max(cntos) as maxos
from q_os_0
group by cityid),

q_os_2 as (select q_os_1.cityid as cityid, os, maxos
from q_os_1 inner join q_os_0 on q_os_1.cityid = q_os_0.cityid and q_os_1.maxos = q_os_0.cntos),

q_browser_0 as (select
  cityid,
  browser,
  count(*) as cntbr
from q_source lateral view ua8(user_agent) uap as device, os, browser
group by cityid, browser),

q_browser_1 as (select cityid, max(cntbr) as maxbr
from q_browser_0
group by cityid),

q_browser_2 as (select q_browser_1.cityid as cityid, browser, maxbr
from q_browser_1 inner join q_browser_0 on q_browser_1.cityid = q_browser_0.cityid and q_browser_1.maxbr = q_browser_0.cntbr)

select city.id, city.name, q_device_2.device, q_os_2.os, q_browser_2.browser
from city right join q_device_2 on city.id = q_device_2.cityid
             full join q_os_2 on q_device_2.cityid = q_os_2.cityid
             full join q_browser_2 on q_device_2.cityid = q_browser_2.cityid
order by city.id;


