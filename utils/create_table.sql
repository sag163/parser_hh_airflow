CREATE TABLE IF NOT EXISTS vacancies (
    id            int NOT NULL PRIMARY KEY,
    name_vacancy     varchar(180),          
    url_vacancy         varchar(280),
    created_at            timestamp default null,
    published_at            timestamp default NULL

);
