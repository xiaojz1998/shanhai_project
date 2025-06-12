---------------------------------------------
-- File: test.sql
-- Time: 2025/6/11 10:30
-- User: xiaoj
-- Description:  
---------------------------------------------
select * from public.dim_test_mainland_drama
order by test_time desc;

select '2025-06-12 24:00:00'::timestamp;


insert into public.dim_test_mainland_drama(drama_id, drama_name, test_time)
values (9314,'【非独家】诱她 When Affair Becomes Affection','2025-06-12 24:00:00'::timestamp),
       (9403,'【非独家】逃不出他掌心 A Sweet Contract with the Mafia Boss','2025-06-12 15:00:00'::timestamp)
