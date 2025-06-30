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
       (9403,'【非独家】逃不出他掌心 A Sweet Contract with the Mafia Boss','2025-06-12 15:00:00'::timestamp);


insert into public.dim_test_mainland_drama(drama_id, drama_name, test_time)
values (9529,'为女儿，我爬了前男友的床','2025-06-17 8:00:00'::timestamp)


insert into public.dim_test_mainland_drama(drama_id, drama_name, test_time)
values (9529,'为女儿，我爬了前男友的床','2025-06-17 8:00:00'::timestamp)


insert into public.dim_test_mainland_drama(drama_id, drama_name, test_time)
values (9234,'【自制非独家】Memory Reset: Mr. Restraint Begged for Love 禁欲老公人设崩了','2025-06-19 19:00:00'::timestamp)

insert into public.dim_test_mainland_drama(drama_id, drama_name, test_time)
values (9936,'【自制独家】Widow to Queen: The Alpha’s Resurrection 开局守寡：改嫁当天前夫回来了','2025-06-21 8:00:00'::timestamp);


insert into public.dim_test_mainland_drama(drama_id, drama_name, test_time)
values (10020,'【非独家】厨神白月光 The Secret Recipe to Snatch a Billionaire','2025-06-24 15:00:00'::timestamp);