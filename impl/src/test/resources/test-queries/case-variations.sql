select distinct
  case pop
    when 0 then "nobody"
    when 1 then "one"
    when 2 then "a couple"
    when 3 then "a few"
    else "more"
  end as cardinal,
  case pop
    when 1 then 0
    when 10 then 1
  end as power,
  case
    when pop % 2 = 0 then "even"
    when pop = 1 or pop = 9 then "odd"
    else "prime"
  end as parity,
  case
    when pop > 5 then pop - 5
  end as grade
  from `zips.data`
  where pop <= 10
  order by pop
