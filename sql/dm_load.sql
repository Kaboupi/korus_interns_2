INSERT INTO dm.finalmart
SELECT
	t.recorded_on AS date,
	t.product_id,
	p.name_short,
	c.category_name,
	b.brand,
	str.pos_name,
	quantity,
	stk.available_quantity,
	ROUND((price_full - price) * 100 / price_full, 2) AS sale,
	price,
	price * quantity AS revenue,
	stk.cost_per_item,
	now()::TIMESTAMP AS load_date
FROM dds.transaction t
LEFT JOIN dds.product p ON p.product_id = t.product_id 
LEFT JOIN dds.category c ON c.category_id = p.category_id 
LEFT JOIN dds.brand b ON b.brand_id = p.brand_id
JOIN dds.stores str ON str.pos = t.pos
JOIN dds.stock stk ON 
    stk.available_on = t.recorded_on::DATE AND
    stk.product_id = t.product_id AND
    stk.pos = t.pos
WHERE NOT EXISTS (
	SELECT 1
	FROM dm.finalmart f
	WHERE f.date = t.recorded_on AND
		f.product_id = t.product_id AND
		f.pos_name = str.pos_name
)