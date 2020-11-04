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
  (LIKE(UPPER(A_Name), "%" || UPPER("Half-Life") || "%", "\\\\")) AND
  (D_Metacritic >= 94) AND
  (C_Recommendations >= 12486)
ORDER BY
  D_Metacritic DESC,
  PriceInitial DESC,
  A_Name ASC
LIMIT 10
