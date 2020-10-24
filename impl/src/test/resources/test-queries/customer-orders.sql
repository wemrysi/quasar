SELECT DISTINCT
  *
FROM (
  SELECT DISTINCT
    row.PurchaseDate AS PurchaseDate,
    row.City AS City,
    row.Name AS Name,
    [row.Brand...] AS Brand,
    [row.Type...] AS Type,
    [row.Size...] AS Size,
    [row.Quantity...] AS Quantity,
    [row.Color...] AS Color,
    [row.UnitPrice...] AS UnitPrice,
    [row.PricePaid...] AS PricePaid,
    SUM(row.TotalPaid) AS TotalPaid
  FROM (
    SELECT
      o.order_key AS OrderKey,
      o.purchase_date AS PurchaseDate,
      c.first_name AS Name,
      c.city AS City,
      c.state AS State,
      i.clothing_size AS Size,
      i.clothing_type AS Type,
      i.clothing_color AS Color,
      i.price AS UnitPrice,
      oi.qty AS Quantity,
      i.price * oi.qty AS PricePaid,
      i.price * oi.qty AS TotalPaid,
      i.clothing_brand AS Brand
    FROM
      `customers.data` AS c INNER JOIN
      `orders.data` AS o ON c.customer_key = o.customer_key INNER JOIN
      `ordered_items.data` AS oi ON oi.order_key = o.order_key INNER JOIN
      `inventory_items.data` AS i ON i.item_key = oi.item_key
    WHERE
      c.state = "OH"
    ORDER BY UnitPrice ASC) AS row
  GROUP BY
    row.PurchaseDate,
    row.City,
    row.Name
  ORDER BY
    row.PurchaseDate ASC,
    row.City ASC,
    row.Name ASC) AS foo
WHERE true
