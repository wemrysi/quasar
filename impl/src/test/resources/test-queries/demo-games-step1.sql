SELECT
  A_Name AS Game,
  B_Released AS ReleaseDate,
  C_Recommendations AS Reviews,
  D_Metacritic AS Score,
  PriceInitial AS Price
FROM
  `steamgames.data`
WHERE
  (IsAction = "TRUE") AND
  (A_Name <> "") AND
  (D_Metacritic >= 0) AND
  (C_Recommendations >= 0)
ORDER BY
  D_Metacritic DESC,
  PriceInitial DESC,
  A_Name ASC
LIMIT 10
