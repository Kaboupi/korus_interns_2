CREATE TABLE IF NOT EXISTS ref_info.error_types(
    error_id VARCHAR(50) PRIMARY KEY,
    error_type VARCHAR(255)
);

INSERT INTO ref_info.error_types
VALUES ('misc', 'Прочие'),
       ('dup_idx', 'Дубликат-индекс'),
       ('dup_val', 'Дубликат-значение'),
       ('inc_val', 'Некорректное значение'),
       ('inc_vlim', 'Некорректное значение (ограничение 40 символов)'),
       ('empty', 'Пустое значение'),
       ('missing', 'Отсутствует связь между таблицами из-за нехватки ключей');