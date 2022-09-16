create_vacancy = '''INSERT INTO
    vacancies
VALUES (id, 'name_vacancy', 'url_vacancy', 'created_at', 'published_at')
ON CONFLICT (id) DO
    NOTHING;'''

check_vacancy = '''select exists(select 1 from vacancies where id=id_number)'''




