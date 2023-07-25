CREATE TABLE IF NOT EXISTS ref_info.dict(
    status_id INTEGER PRIMARY KEY,
    status_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS ref_info.error_types(
    error_id VARCHAR(50) PRIMARY KEY,
    error_type VARCHAR(255)
);

INSERT INTO ref_info.dict
VALUES (1, 'Закупка необходима, так как продажи за отчётный период превышают допустимый остаток'),
       (2, 'Закупка необходима, так как остаток на складе ниже критического минимума'),
       (3, 'Закупка необходима, так как предвещается сезонный спрос'),
       (4, 'Закупка не требуется');

INSERT INTO ref_info.error_types
VALUES ('misc', 'Прочие'),
       ('dup_idx', 'Дубликат-индекс'),
       ('dup_val', 'Дубликат-значение'),
       ('inc_val', 'Некорректное значение'),
       ('inc_vlim', 'Некорректное значение (ограничение 40 символов)'),
       ('empty', 'Пустое значение'),
       ('missing', 'Отсутствует связь между таблицами из-за нехватки ключей');