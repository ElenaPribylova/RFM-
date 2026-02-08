-----01 KPI базы (bonuscheques)

select
  min(datetime)::date                    as "Начало периода",
  max(datetime)::date                    as "Конец периода",
  (max(datetime)::date - min(datetime)::date)
                                          as "Дней в периоде",
  count(distinct card)                   as "Клиентов",
  count(*)                               as "Покупок",
  round(sum(summ_with_disc)::numeric, 2) as "Выручка"
from bonuscheques
where card is not null
and {{date_range}}
and {{shop}};

--------------------------------------------------------------------------------------------


----02 Покупки по месяцам и аптекам

select
  to_char(datetime, 'YYYY-MM') as "Месяц",
  shop                          as "Аптека",
  count(*)                      as "Покупок"
from bonuscheques
where card is not null
and {{date_range}}
and {{shop}}
group by 1, 2
order by 1, 2;


--------------------------------------------------------------------------------------------


----03 RFM метрики по клиентам


with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  [[and {{date_range}}]]
  [[and {{shop}}]]
),
anchor as (
  select max(dt) as anchor_dt from t
)
select
  card,
  (select anchor_dt from anchor) - max(dt) as for_r,
  count(*) as for_f,
  sum(amount) as for_m
from t
group by card
order by card;


--------------------------------------------------------------------------------------------


----04 Пороги RFM (перцентили + ABC)


with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (
  select max(dt) as anchor_dt from t
),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
abc as (
  select
    card,
    for_r,
    for_f,
    for_m,
    sum(for_m) over() as total_m,
    sum(for_m) over(order by for_m desc) as cum_m
  from rfm
)
select
  percentile_disc(0.2) within group (order by for_r)
      as "R: 20-й перцентиль (дней)",
  percentile_disc(0.4) within group (order by for_r)
      as "R: 40-й перцентиль (дней)",
  percentile_disc(0.6) within group (order by for_r)
      as "R: 60-й перцентиль (дней)",
  percentile_disc(0.8) within group (order by for_r)
      as "R: 80-й перцентиль (дней)",

  percentile_disc(0.33) within group (order by for_f)
      as "F: нижняя граница (33%)",
  percentile_disc(0.66) within group (order by for_f)
      as "F: верхняя граница (66%)",

  percentile_disc(0.33) within group (order by for_m)
      as "M: нижняя граница (33%)",
  percentile_disc(0.66) within group (order by for_m)
      as "M: верхняя граница (66%)",

  round(
    100.0 * count(*) filter (where cum_m / total_m <= 0.80) / count(*),
    2
  ) as "ABC: клиенты A, %",

  round(
    100.0 * count(*) filter (
      where cum_m / total_m > 0.80 and cum_m / total_m <= 0.95
    ) / count(*),
    2
  ) as "ABC: клиенты B, %",

  round(
    100.0 * count(*) filter (where cum_m / total_m > 0.95) / count(*),
    2
  ) as "ABC: клиенты C, %"
from abc;

--------------------------------------------------------------------------------------------


----04a Пороги R (Recency)

with t as (
  select card, shop, datetime::date as dt, summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
)
select
  percentile_disc(0.2) within group (order by for_r) as "R: 20-й перцентиль (дней)",
  percentile_disc(0.4) within group (order by for_r) as "R: 40-й перцентиль (дней)",
  percentile_disc(0.6) within group (order by for_r) as "R: 60-й перцентиль (дней)",
  percentile_disc(0.8) within group (order by for_r) as "R: 80-й перцентиль (дней)"
from rfm;

--------------------------------------------------------------------------------------------


----04b — Frequency (границы F)

with t as (
  select card, shop, datetime::date as dt, summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
)
select
  percentile_disc(0.33) within group (order by for_f) as "F: нижняя граница (33%)",
  percentile_disc(0.66) within group (order by for_f) as "F: верхняя граница (66%)"
from rfm;

--------------------------------------------------------------------------------------------


----04c — Monetary (границы M)

with t as (
  select card, shop, datetime::date as dt, summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
)
select
  percentile_disc(0.33) within group (order by for_m) as "M: нижняя граница (33%)",
  percentile_disc(0.66) within group (order by for_m) as "M: верхняя граница (66%)"
from rfm;


--------------------------------------------------------------------------------------------


----04d ABC по выручке (доли клиентов)

with t as (
  select card, shop, datetime::date as dt, summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
abc as (
  select
    card,
    for_m,
    sum(for_m) over() as total_m,
    sum(for_m) over(order by for_m desc) as cum_m
  from rfm
)
select
  round(100.0 * count(*) filter (where cum_m/total_m <= 0.80) / count(*), 2) as "ABC: клиенты A, %",
  round(100.0 * count(*) filter (where cum_m/total_m > 0.80 and cum_m/total_m <= 0.95) / count(*), 2) as "ABC: клиенты B, %",
  round(100.0 * count(*) filter (where cum_m/total_m > 0.95) / count(*), 2) as "ABC: клиенты C, %"
from abc;

--------------------------------------------------------------------------------------------


----05 Клиенты RFM (баллы + сегменты)

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (
  select max(dt) as anchor_dt from t
),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  card                                   as "Клиент (card)",
  for_r                                  as "Давность, дней",
  for_f                                  as "Частота покупок",
  round(for_m::numeric, 2)               as "Сумма покупок",
  r_score                                as "R",
  f_score                                as "F",
  m_score                                as "M",
  (r_score::text || f_score::text || m_score::text)
                                         as "RFM-код",
  segment                                as "Сегмент"
from seg
order by "Сегмент", "Сумма покупок" desc;

--------------------------------------------------------------------------------------------


----06 Сводка по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06a Клиенты по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06b Выручка по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06в Клиенты по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06г Выручка по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06д выручка на клиента по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06е Средний чек по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06ж Давность (дней) по клиентам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06з Частота покупок по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------


----06и Доля выручки, % по сегментам

with t as (
  select
    card,
    shop,
    datetime::date as dt,
    summ_with_disc as amount
  from bonuscheques
  where card is not null
  and {{date_range}}
  and {{shop}}
),
anchor as (select max(dt) as anchor_dt from t),
rfm as (
  select
    card,
    (select anchor_dt from anchor) - max(dt) as for_r,
    count(*) as for_f,
    sum(amount) as for_m
  from t
  group by card
),
points as (
  select
    percentile_disc(0.2) within group (order by for_r) as r_p20,
    percentile_disc(0.4) within group (order by for_r) as r_p40,
    percentile_disc(0.6) within group (order by for_r) as r_p60,
    percentile_disc(0.8) within group (order by for_r) as r_p80,
    percentile_disc(0.33) within group (order by for_f) as f_p33,
    percentile_disc(0.66) within group (order by for_f) as f_p66,
    percentile_disc(0.33) within group (order by for_m) as m_p33,
    percentile_disc(0.66) within group (order by for_m) as m_p66
  from rfm
),
scored as (
  select
    rfm.*,
    case
      when for_r <= r_p20 then 5
      when for_r <= r_p40 then 4
      when for_r <= r_p60 then 3
      when for_r <= r_p80 then 2
      else 1
    end as r_score,
    case
      when for_f >= f_p66 then 3
      when for_f >= f_p33 then 2
      else 1
    end as f_score,
    case
      when for_m >= m_p66 then 3
      when for_m >= m_p33 then 2
      else 1
    end as m_score
  from rfm
  cross join points
),
seg as (
  select
    *,
    case
      when r_score = 5 and f_score = 3 and m_score = 3 then 'VIP'
      when r_score >= 4 and f_score = 3 and m_score >= 2 then 'Лояльные'
      when r_score >= 4 and f_score = 2 and m_score >= 2 then 'Потенциал роста'
      when r_score = 5 and f_score = 1 and m_score >= 2 then 'Новички перспективные'
      when r_score = 5 and f_score = 1 and m_score = 1 then 'Новички низкой ценности'
      when r_score in (2,3) and f_score = 3 then 'В зоне риска'
      when r_score <= 2 and f_score <= 2 then 'Спящие'
      when r_score = 1 and f_score = 1 and m_score = 1 then 'Потерянные'
      else 'Прочие'
    end as segment
  from scored
)
select
  segment                                   as "Сегмент",
  count(*)                                 as "Клиентов",
  round(sum(for_m)::numeric, 2)             as "Выручка сегмента",
  round(avg(for_m)::numeric, 2)             as "Выручка на клиента",
  round(sum(for_m)::numeric / nullif(sum(for_f),0), 2)
                                            as "Средний чек (оценка)",
  round(avg(for_r)::numeric, 1)             as "Давность, дней",
  round(avg(for_f)::numeric, 2)             as "Частота покупок",
  round(
    100.0 * sum(for_m)
    / nullif((select sum(for_m) from seg),0),
    2
  )                                         as "Доля выручки, %"
from seg
group by 1
order by "Выручка сегмента" desc;


--------------------------------------------------------------------------------------------
