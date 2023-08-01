----------------------------
-- Мониторинг продаж
----------------------------

-- Общая выручка
SELECT SUM(revenue)
FROM dm.finalmart

-- Средний чек
SELECT AVG(revenue)
FROM dm.finalmart

-- Средняя скидка
SELECT AVG(sale)
FROM dm.finalmart

-- Объем продаж
SELECT SUM(quantity)
FROM dm.finalmart

-- Распределение выручки по магазинам
SELECT pos_name, SUM(revenue)
FROM dm.finalmart
GROUP BY pos_name

-- График выручки
SELECT date::DATE, pos_name, SUM(revenue)
FROM dm.finalmart
GROUP BY 1, 2
ORDER BY 1

-- Топ продуктов по прибыли от продаж
SELECT 
	category_name AS "Название категории",
	name_short AS "Название продукта",
	pos_name AS "Магазин",
	SUM(revenue) AS "Выручка"
FROM dm.finalmart
GROUP BY category_name, name_short, pos_name 
ORDER BY "Выручка" DESC

-- Дневная динамика выручки
SELECT 
  DATE_TRUNC('day', date)::DATE AS "date",
  SUM(revenue) AS "Общая выручка"
FROM dm.finalmart
GROUP BY 1
ORDER BY 1

-- Недельная динамика выручки
SELECT 
  DATE_TRUNC('week', date)::DATE AS "date",
  SUM(revenue) AS "Общая выручка"
FROM dm.finalmart
GROUP BY 1
ORDER BY 1

-- Месячная динамика выручки
SELECT 
  DATE_TRUNC('month', date)::DATE AS "date",
  SUM(revenue) AS "Общая выручка"
FROM dm.finalmart
GROUP BY 1
ORDER BY 1

-- Топ 10 лучших категорий
SELECT 
    category_name AS "Категория", 
    sum(revenue) AS "Выручка"
FROM dm.finalmart
GROUP BY category_name 
ORDER BY "Общая выручка" DESC
LIMIT 10

-- Топ 10 худших категорий
SELECT 
    category_name AS "Категория", 
    sum(revenue) AS "Выручка"
FROM dm.finalmart
GROUP BY category_name 
ORDER BY "Общая выручка" ASC
LIMIT 10

-- Топ 10 лучших продуктов по выбранной категории
SELECT 
    name_short AS "Наименование продукта", 
    sum(revenue) AS "Выручка"
FROM dm.finalmart
GROUP BY category_name 
ORDER BY "Общая выручка" DESC
LIMIT 10


-- Топ 10 лучших продуктов по выбранной категории
SELECT 
    name_short AS "Наименование продукта", 
    sum(revenue) AS "Выручка"
FROM dm.finalmart
GROUP BY category_name 
ORDER BY "Общая выручка" ASC
LIMIT 10

----------------------------
-- Рекомендации к закупке
----------------------------

-- Сводная таблица продаж и остатков
SELECT 
  product_id AS "ID продукта", 
  name_short AS "Название продукта", 
  pos_name AS "Магазин", 
  category_name AS "Категория", 
  cost_per_item AS "Себестоимость", 
  sum(quantity) AS "Общее кол-во продаж", 
  min(available_quantity) AS "Осталось", 
  CASE
   WHEN min(available_quantity) <= 5 or min(available_quantity) is NULL THEN 'Необходима закупка. Критический минимум товара' 
   WHEN sum(quantity) > min(available_quantity) THEN 'Необходима закупка. Повышенный спрос' 
   ELSE 'Закупка не требуется' 
   END AS "Статус закупки" 
FROM dm.finalmart
GROUP BY  
  product_id, 
  pos_name, 
  name_short, 
  category_name, 
  cost_per_item

-- Соотношение статусов закупок
with subquery AS (
	SELECT 
	product_id AS "ID продукта", 
  	name_short AS "Название продукта", 
  	pos_name AS "Магазин", 
  	category_name AS "Категория", 
  	cost_per_item AS "Себестоимость", 
  	sum(quantity) AS "Общее кол-во продаж", 
  	min(available_quantity) AS "Осталось", 
  	CASE
   		WHEN min(available_quantity) <= 5 or min(available_quantity) is NULL THEN 'Необходима закупка. Критический минимум товара' 
   		WHEN sum(quantity) > min(available_quantity) THEN 'Необходима закупка. Повышенный спрос' 
   		ELSE 'Закупка не требуется' 
   	END AS "status"
	FROM dm.finalmart
	GROUP BY  
  	product_id, 
  	pos_name, 
  	name_short, 
  	category_name, 
  	cost_per_item
)
SELECT
	status AS "Статус закупки",
	COUNT(status) AS "Требуемое количество"
FROM subquery
GROUP BY 1

-- Объём продаж товара (manager_dashboard)
SELECT
	date::DATE AS "Дата",
	name_short AS "Название продукта",
	sum(quantity) AS "Объем продаж"
FROM dm.finalmart
GROUP BY 1, 2
ORDER BY 1, 3, 2

-- Остаток товара по магазину
SELECT 
	date::DATE AS "Дата",
	name_short AS "Название продукта", 
	sum(available_quantity) AS "Доступное количество"
FROM dm.finalmart
GROUP BY 1, 2
ORDER BY 1

-- Объём продаж товара (pos_dashboard)
SELECT
	date::DATE AS "Дата",
	category_name AS "Категория",
	name_short AS "Название продукта",
	sum(available_quantity) AS "Объем продаж"
FROM dm.finalmart
GROUP BY 1, 2, 3
ORDER BY 1
